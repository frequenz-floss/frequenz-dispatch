# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""The dispatch background service."""

from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from heapq import heappop, heappush
from typing import Callable

import grpc.aio
from frequenz.channels import Broadcast, Receiver, select, selected_from
from frequenz.channels.timer import SkipMissedAndResync, Timer
from frequenz.client.dispatch import Client
from frequenz.client.dispatch.types import Event
from frequenz.sdk.actor import BackgroundService

from ._dispatch import Dispatch
from ._event import Created, Deleted, DispatchEvent, Updated

_logger = logging.getLogger(__name__)
"""The logger for this module."""


class _MergeStrategy(ABC):
    """Base class for strategies to merge running intervals."""

    @abstractmethod
    def _get_filter_function(
        self,
        scheduler: DispatchScheduler,
    ) -> Callable[[Dispatch], bool]:
        """Get a filter function for dispatches.

        Args:
            scheduler: The dispatch scheduler.

        Returns:
            A filter function.
        """


class MergeByType(_MergeStrategy):
    """Merge running intervals based on the dispatch type."""

    def __init__(self) -> None:
        """Initialize the strategy."""
        self._scheduler: DispatchScheduler
        self._new_dispatch: Dispatch

    def _get_filter_function(
        self, scheduler: DispatchScheduler
    ) -> Callable[[Dispatch], bool]:
        """Get a filter function for dispatches."""
        self._scheduler = scheduler
        return self._filter_func

    def _criteria(self, dispatch: Dispatch) -> bool:
        """Define the criteria for checking other running dispatches."""
        return dispatch.type == self._new_dispatch.type

    def _filter_func(self, new_dispatch: Dispatch) -> bool:
        """Filter dispatches based on the merge strategy.

        Keeps start events.
        Keeps stop events only if no other dispatches matching the
        strategy's criteria are running.
        """
        if new_dispatch.started:
            return True

        self._new_dispatch = new_dispatch

        # pylint: disable=protected-access
        other_dispatches_running = any(
            dispatch.started
            for dispatch in self._scheduler._dispatches.values()
            if self._criteria(dispatch)
        )
        # pylint: enable=protected-access

        return not other_dispatches_running


class MergeByTypeTarget(MergeByType):
    """Merge running intervals based on the dispatch type and target."""

    def _criteria(self, dispatch: Dispatch) -> bool:
        """Define the criteria for checking other running dispatches."""
        return (
            dispatch.type == self._new_dispatch.type
            and dispatch.target == self._new_dispatch.target
        )


# pylint: disable=too-many-instance-attributes
class DispatchScheduler(BackgroundService):
    """Dispatch background service.

    This service is responsible for managing dispatches and scheduling them
    based on their start and stop times.
    """

    @dataclass(order=True)
    class QueueItem:
        """A queue item for the scheduled events.

        This class is used to define the order of the queue items based on the
        scheduled time, whether it is a stop event and finally the dispatch ID
        (for uniqueness).
        """

        time: datetime
        priority: int
        """Sort priority when the time is the same.

        Exists to make sure that when two events are scheduled at the same time,
        the start event is executed first, allowing filters down the data pipeline
        to consider the start event when deciding whether to execute the
        stop event.
        """
        dispatch_id: int
        dispatch: Dispatch = field(compare=False)

        def __init__(
            self, time: datetime, dispatch: Dispatch, stop_event: bool
        ) -> None:
            """Initialize the queue item."""
            self.time = time
            self.dispatch_id = dispatch.id
            self.priority = int(stop_event)
            self.dispatch = dispatch

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        microgrid_id: int,
        client: Client,
    ) -> None:
        """Initialize the background service.

        Args:
            microgrid_id: The microgrid ID to handle dispatches for.
            client: The client to use for fetching dispatches.
        """
        super().__init__(name="dispatch")

        self._client = client
        self._dispatches: dict[int, Dispatch] = {}
        self._microgrid_id = microgrid_id

        self._lifecycle_events_channel = Broadcast[DispatchEvent](
            name="lifecycle_events"
        )
        self._lifecycle_events_tx = self._lifecycle_events_channel.new_sender()
        self._running_state_status_channel = Broadcast[Dispatch](
            name="running_state_status"
        )

        self._running_state_status_tx = self._running_state_status_channel.new_sender()
        self._next_event_timer = Timer(
            timedelta(seconds=100), SkipMissedAndResync(), auto_start=False
        )
        """The timer to schedule the next event.

        Interval is chosen arbitrarily, as it will be reset on the first event.
        """

        self._scheduled_events: list["DispatchScheduler.QueueItem"] = []
        """The scheduled events, sorted by time.

        Each event is a tuple of the scheduled time and the dispatch.
        heapq is used to keep the list sorted by time, so the next event is
        always at index 0.
        """

    # pylint: disable=redefined-builtin
    def new_lifecycle_events_receiver(self, type: str) -> Receiver[DispatchEvent]:
        """Create a new receiver for lifecycle events.

        Args:
            type: The type of events to receive.

        Returns:
            A new receiver for lifecycle events.
        """
        return self._lifecycle_events_channel.new_receiver().filter(
            lambda event: event.dispatch.type == type
        )

    async def new_running_state_event_receiver(
        self, type: str, *, merge_strategy: _MergeStrategy | None = None
    ) -> Receiver[Dispatch]:
        """Create a new receiver for running state events of the specified type.

        `merge_strategy` can be one of `MergeByType` or `MergeByTypeTarget`.
        If set, running intervals from multiple dispatches will be merged,
        depending on the chosen strategy.
        When merging, stop events are ignored as long as at least one
        merge-criteria-matching dispatch remains active.

        Args:
            type: The type of events to receive.
            merge_strategy: The merge strategy to use.
        Returns:
            A new receiver for running state status.
        """
        dispatches = [
            dispatch for dispatch in self._dispatches.values() if dispatch.type == type
        ]

        receiver = self._running_state_status_channel.new_receiver(
            limit=max(1, len(dispatches))
        ).filter(lambda dispatch: dispatch.type == type)

        if merge_strategy:
            # pylint: disable=protected-access
            receiver = receiver.filter(merge_strategy._get_filter_function(self))
            # pylint: enable=protected-access

        for dispatch in dispatches:
            await self._send_running_state_change(dispatch)

        return receiver

    # pylint: enable=redefined-builtin

    def start(self) -> None:
        """Start the background service."""
        self._tasks.add(asyncio.create_task(self._run()))

    async def _run(self) -> None:
        """Run the background service."""
        _logger.info(
            "Starting dispatching background service for microgrid %s",
            self._microgrid_id,
        )

        # Initial fetch
        await self._fetch()

        stream = self._client.stream(microgrid_id=self._microgrid_id)

        # Streaming updates
        async for selected in select(self._next_event_timer, stream):
            if selected_from(selected, self._next_event_timer):
                if not self._scheduled_events:
                    continue
                _logger.debug(
                    "Executing scheduled event: %s", self._scheduled_events[0].dispatch
                )
                await self._execute_scheduled_event(
                    heappop(self._scheduled_events).dispatch
                )
            elif selected_from(selected, stream):
                _logger.debug("Received dispatch event: %s", selected.message)
                dispatch = Dispatch(selected.message.dispatch)
                match selected.message.event:
                    case Event.CREATED:
                        self._dispatches[dispatch.id] = dispatch
                        await self._update_dispatch_schedule_and_notify(dispatch, None)
                        await self._lifecycle_events_tx.send(Created(dispatch=dispatch))
                    case Event.UPDATED:
                        await self._update_dispatch_schedule_and_notify(
                            dispatch, self._dispatches[dispatch.id]
                        )
                        self._dispatches[dispatch.id] = dispatch
                        await self._lifecycle_events_tx.send(Updated(dispatch=dispatch))
                    case Event.DELETED:
                        self._dispatches.pop(dispatch.id)
                        await self._update_dispatch_schedule_and_notify(None, dispatch)

                        await self._lifecycle_events_tx.send(Deleted(dispatch=dispatch))

    async def _execute_scheduled_event(self, dispatch: Dispatch) -> None:
        """Execute a scheduled event.

        Args:
            dispatch: The dispatch to execute.
        """
        await self._send_running_state_change(dispatch)

        # The timer is always a tiny bit delayed, so we need to check if the
        # actor is supposed to be running now (we're assuming it wasn't already
        # running, as all checks are done before scheduling)
        if dispatch.started:
            # If it should be running, schedule the stop event
            self._schedule_stop(dispatch)
        # If the actor is not running, we need to schedule the next start
        else:
            self._schedule_start(dispatch)

        self._update_timer()

    async def _fetch(self) -> None:
        """Fetch all relevant dispatches using list.

        This is used for the initial fetch and for re-fetching all dispatches
        if the connection was lost.
        """
        old_dispatches = self._dispatches
        self._dispatches = {}

        try:
            _logger.info("Fetching dispatches for microgrid %s", self._microgrid_id)
            async for page in self._client.list(microgrid_id=self._microgrid_id):
                for client_dispatch in page:
                    dispatch = Dispatch(client_dispatch)

                    self._dispatches[dispatch.id] = Dispatch(client_dispatch)
                    old_dispatch = old_dispatches.pop(dispatch.id, None)
                    if not old_dispatch:
                        _logger.debug("New dispatch: %s", dispatch)
                        await self._update_dispatch_schedule_and_notify(dispatch, None)
                        await self._lifecycle_events_tx.send(Created(dispatch=dispatch))
                    elif dispatch.update_time != old_dispatch.update_time:
                        _logger.debug("Updated dispatch: %s", dispatch)
                        await self._update_dispatch_schedule_and_notify(
                            dispatch, old_dispatch
                        )
                        await self._lifecycle_events_tx.send(Updated(dispatch=dispatch))

            _logger.info("Received %s dispatches", len(self._dispatches))

        except grpc.aio.AioRpcError as error:
            _logger.error("Error fetching dispatches: %s", error)
            self._dispatches = old_dispatches
            return

        for dispatch in old_dispatches.values():
            _logger.info("Deleted dispatch: %s", dispatch)
            await self._lifecycle_events_tx.send(Deleted(dispatch=dispatch))
            await self._update_dispatch_schedule_and_notify(None, dispatch)

            # Set deleted only here as it influences the result of dispatch.started
            # which is used in above in _running_state_change
            dispatch._set_deleted()  # pylint: disable=protected-access
            await self._lifecycle_events_tx.send(Deleted(dispatch=dispatch))

    async def _update_dispatch_schedule_and_notify(
        self, dispatch: Dispatch | None, old_dispatch: Dispatch | None
    ) -> None:
        """Update the schedule for a dispatch.

        Schedules, reschedules or cancels the dispatch events
        based on the start_time and active status.

        Sends a running state change notification if necessary.

        For example:
            * when the start_time changes, the dispatch is rescheduled
            * when the dispatch is deactivated, the dispatch is cancelled

        Args:
            dispatch: The dispatch to update the schedule for.
            old_dispatch: The old dispatch, if available.
        """
        # If dispatch is None, the dispatch was deleted
        # and we need to cancel any existing event for it
        if not dispatch and old_dispatch:
            self._remove_scheduled(old_dispatch)

            was_running = old_dispatch.started
            old_dispatch._set_deleted()  # pylint: disable=protected-access)

            # If the dispatch was running, we need to notify
            if was_running:
                await self._send_running_state_change(old_dispatch)

        # A new dispatch was created
        elif dispatch and not old_dispatch:
            assert not self._remove_scheduled(
                dispatch
            ), "New dispatch already scheduled?!"
            # If its currently running, send notification right away
            if dispatch.started:
                await self._send_running_state_change(dispatch)

                self._schedule_stop(dispatch)
            # Otherwise, if it's enabled but not yet running, schedule it
            else:
                self._schedule_start(dispatch)

        # Dispatch was updated
        elif dispatch and old_dispatch:
            # Remove potentially existing scheduled event
            self._remove_scheduled(old_dispatch)

            # Check if the change requires an immediate notification
            if self._update_changed_running_state(dispatch, old_dispatch):
                await self._send_running_state_change(dispatch)

            if dispatch.started:
                self._schedule_stop(dispatch)
            else:
                self._schedule_start(dispatch)

        # We modified the schedule, so we need to reset the timer
        self._update_timer()

    def _update_timer(self) -> None:
        """Update the timer to the next event."""
        if self._scheduled_events:
            due_at: datetime = self._scheduled_events[0].time
            self._next_event_timer.reset(interval=due_at - datetime.now(timezone.utc))
            _logger.debug("Next event scheduled at %s", self._scheduled_events[0].time)

    def _remove_scheduled(self, dispatch: Dispatch) -> bool:
        """Remove a dispatch from the scheduled events.

        Args:
            dispatch: The dispatch to remove.

        Returns:
            True if the dispatch was found and removed, False otherwise.
        """
        for idx, item in enumerate(self._scheduled_events):
            if dispatch.id == item.dispatch.id:
                self._scheduled_events.pop(idx)
                return True

        return False

    def _schedule_start(self, dispatch: Dispatch) -> None:
        """Schedule a dispatch to start.

        Args:
            dispatch: The dispatch to schedule.
        """
        # If the dispatch is not active, don't schedule it
        if not dispatch.active:
            return

        # Schedule the next run
        try:
            if next_run := dispatch.next_run:
                heappush(
                    self._scheduled_events,
                    self.QueueItem(next_run, dispatch, stop_event=False),
                )
                _logger.debug(
                    "Scheduled dispatch %s to start at %s", dispatch.id, next_run
                )
            else:
                _logger.debug("Dispatch %s has no next run", dispatch.id)
        except ValueError as error:
            _logger.error("Error scheduling dispatch %s: %s", dispatch.id, error)

    def _schedule_stop(self, dispatch: Dispatch) -> None:
        """Schedule a dispatch to stop.

        Args:
            dispatch: The dispatch to schedule.
        """
        # Setup stop timer if the dispatch has a duration
        if dispatch.duration and dispatch.duration > timedelta(seconds=0):
            until = dispatch.until
            assert until is not None
            heappush(
                self._scheduled_events, self.QueueItem(until, dispatch, stop_event=True)
            )
            _logger.debug("Scheduled dispatch %s to stop at %s", dispatch, until)

    def _update_changed_running_state(
        self, updated_dispatch: Dispatch, previous_dispatch: Dispatch
    ) -> bool:
        """Check if the running state of a dispatch has changed.

        Checks if any of the running state changes to the dispatch
        require a new message to be sent to the actor so that it can potentially
        change its runtime configuration or start/stop itself.

        Also checks if a dispatch update was not sent due to connection issues
        in which case we need to send the message now.

        Args:
            updated_dispatch: The new dispatch
            previous_dispatch: The old dispatch

        Returns:
            True if the running state has changed, False otherwise.
        """
        # If any of the runtime attributes changed, we need to send a message
        runtime_state_attributes = [
            "started",
            "type",
            "target",
            "duration",
            "dry_run",
            "payload",
        ]

        for attribute in runtime_state_attributes:
            if getattr(updated_dispatch, attribute) != getattr(
                previous_dispatch, attribute
            ):
                return True

        return False

    async def _send_running_state_change(self, dispatch: Dispatch) -> None:
        """Send a running state change message.

        Args:
            dispatch: The dispatch that changed.
        """
        await self._running_state_status_tx.send(dispatch)