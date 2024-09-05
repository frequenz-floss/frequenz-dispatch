# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""The dispatch actor."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone

import grpc.aio
from frequenz.channels import Sender
from frequenz.channels.timer import SkipMissedAndDrift, Timer
from frequenz.client.dispatch import Client
from frequenz.sdk.actor import Actor

from ._dispatch import Dispatch, RunningState
from ._event import Created, Deleted, DispatchEvent, Updated

_MAX_AHEAD_SCHEDULE = timedelta(hours=5)
"""The maximum time ahead to schedule a dispatch.

We don't want to schedule dispatches too far ahead,
as they could start drifting if the delay is too long.

This also prevents us from scheduling too many dispatches at once.

The exact value is not important, but should be a few hours and not more than a day.
"""

_DEFAULT_POLL_INTERVAL = timedelta(seconds=10)
"""The default interval to poll the API for dispatch changes."""

_logger = logging.getLogger(__name__)
"""The logger for this module."""


class DispatchingActor(Actor):
    """Dispatch actor.

    This actor is responsible for handling dispatches for a microgrid.

    This means staying in sync with the API and scheduling
    dispatches as necessary.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        microgrid_id: int,
        client: Client,
        lifecycle_updates_sender: Sender[DispatchEvent],
        running_state_change_sender: Sender[Dispatch],
        poll_interval: timedelta = _DEFAULT_POLL_INTERVAL,
    ) -> None:
        """Initialize the actor.

        Args:
            microgrid_id: The microgrid ID to handle dispatches for.
            client: The client to use for fetching dispatches.
            lifecycle_updates_sender: A sender for dispatch lifecycle events.
            running_state_change_sender: A sender for dispatch running state changes.
            poll_interval: The interval to poll the API for dispatche changes.
        """
        super().__init__(name="dispatch")

        self._client = client
        self._dispatches: dict[int, Dispatch] = {}
        self._scheduled: dict[int, asyncio.Task[None]] = {}
        self._microgrid_id = microgrid_id
        self._lifecycle_updates_sender = lifecycle_updates_sender
        self._running_state_change_sender = running_state_change_sender
        self._poll_timer = Timer(poll_interval, SkipMissedAndDrift())

    async def _run(self) -> None:
        """Run the actor."""
        self._poll_timer.reset()
        try:
            async for _ in self._poll_timer:
                await self._fetch()
        except asyncio.CancelledError:
            for task in self._scheduled.values():
                task.cancel()
            raise

    async def _fetch(self) -> None:
        """Fetch all relevant dispatches."""
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
                        self._update_dispatch_schedule(dispatch, None)
                        _logger.info("New dispatch: %s", dispatch)
                        await self._lifecycle_updates_sender.send(
                            Created(dispatch=dispatch)
                        )
                    elif dispatch.update_time != old_dispatch.update_time:
                        self._update_dispatch_schedule(dispatch, old_dispatch)
                        _logger.info("Updated dispatch: %s", dispatch)
                        await self._lifecycle_updates_sender.send(
                            Updated(dispatch=dispatch)
                        )

                        if self._running_state_change(dispatch, old_dispatch):
                            await self._send_running_state_change(dispatch)

        except grpc.aio.AioRpcError as error:
            _logger.error("Error fetching dispatches: %s", error)
            self._dispatches = old_dispatches
            return

        for dispatch in old_dispatches.values():
            _logger.info("Deleted dispatch: %s", dispatch)
            dispatch._set_deleted()  # pylint: disable=protected-access
            await self._lifecycle_updates_sender.send(Deleted(dispatch=dispatch))
            if task := self._scheduled.pop(dispatch.id, None):
                task.cancel()

            if self._running_state_change(None, dispatch):
                await self._send_running_state_change(dispatch)

    def _update_dispatch_schedule(
        self, dispatch: Dispatch, old_dispatch: Dispatch | None
    ) -> None:
        """Update the schedule for a dispatch.

        Schedules, reschedules or cancels the dispatch based on the start_time
        and active status.

        For example:
            * when the start_time changes, the dispatch is rescheduled
            * when the dispatch is deactivated, the dispatch is cancelled

        Args:
            dispatch: The dispatch to update the schedule for.
            old_dispatch: The old dispatch, if available.
        """
        if (
            old_dispatch
            and old_dispatch.active
            and old_dispatch.start_time != dispatch.start_time
        ):
            if task := self._scheduled.pop(dispatch.id, None):
                task.cancel()

        if dispatch.active and dispatch.id not in self._scheduled:
            self._scheduled[dispatch.id] = asyncio.create_task(
                self._schedule_task(dispatch)
            )

    async def _schedule_task(self, dispatch: Dispatch) -> None:
        """Wait for a dispatch to become ready.

        Waits for the dispatches next run and then notifies that it is ready.

        Args:
            dispatch: The dispatch to schedule.
        """

        def next_run_info() -> tuple[datetime, datetime] | None:
            now = datetime.now(tz=timezone.utc)
            next_run = dispatch.next_run_after(now)

            if next_run is None:
                return None

            return now, next_run

        while pair := next_run_info():
            now, next_time = pair

            if next_time - now > _MAX_AHEAD_SCHEDULE:
                await asyncio.sleep(_MAX_AHEAD_SCHEDULE.total_seconds())
                continue

            _logger.info("Dispatch %s scheduled for %s", dispatch.id, next_time)
            await asyncio.sleep((next_time - now).total_seconds())

            _logger.info("Dispatch ready: %s", dispatch)
            await self._running_state_change_sender.send(dispatch)

        _logger.info("Dispatch finished: %s", dispatch)
        self._scheduled.pop(dispatch.id)

    def _running_state_change(
        self, updated_dispatch: Dispatch | None, previous_dispatch: Dispatch | None
    ) -> bool:
        """Check if the running state of a dispatch has changed.

        Checks if any of the running state changes to the dispatch
        require a new message to be sent to the actor so that it can potentially
        change its runtime configuration or start/stop itself.

        Also checks if a dispatch update was not sent due to connection issues
        in which case we need to send the message now.

        Args:
            updated_dispatch: The new dispatch, if available.
            previous_dispatch: The old dispatch, if available.

        Returns:
            True if the running state has changed, False otherwise.
        """
        # New dispatch
        if previous_dispatch is None:
            assert updated_dispatch is not None

            # Client was not informed about the dispatch, do it now
            # pylint: disable=protected-access
            if not updated_dispatch._running_status_notified:
                return True

        # Deleted dispatch
        if updated_dispatch is None:
            assert previous_dispatch is not None
            return (
                previous_dispatch.running(previous_dispatch.type)
                == RunningState.RUNNING
            )

        # If any of the runtime attributes changed, we need to send a message
        runtime_state_attributes = [
            "running",
            "type",
            "selector",
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
        await self._running_state_change_sender.send(dispatch)
        # Update the last sent notification time
        # so we know if this change was already sent
        dispatch._set_running_status_notified()  # pylint: disable=protected-access
