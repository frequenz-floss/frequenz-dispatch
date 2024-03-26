# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""The dispatch actor."""

import asyncio
import logging
from datetime import datetime, timedelta, timezone
from typing import cast

import grpc.aio
from dateutil import rrule
from frequenz.channels import Sender
from frequenz.client.dispatch import Client
from frequenz.client.dispatch.types import Dispatch, Frequency, Weekday
from frequenz.sdk.actor import Actor

from frequenz.dispatch._event import Created, Deleted, DispatchEvent, Updated

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


_RRULE_FREQ_MAP = {
    Frequency.MINUTELY: rrule.MINUTELY,
    Frequency.HOURLY: rrule.HOURLY,
    Frequency.DAILY: rrule.DAILY,
    Frequency.WEEKLY: rrule.WEEKLY,
    Frequency.MONTHLY: rrule.MONTHLY,
}
"""To map from our Frequency enum to the dateutil library enum."""

_RRULE_WEEKDAY_MAP = {
    Weekday.MONDAY: rrule.MO,
    Weekday.TUESDAY: rrule.TU,
    Weekday.WEDNESDAY: rrule.WE,
    Weekday.THURSDAY: rrule.TH,
    Weekday.FRIDAY: rrule.FR,
    Weekday.SATURDAY: rrule.SA,
    Weekday.SUNDAY: rrule.SU,
}
"""To map from our Weekday enum to the dateutil library enum."""


class DispatchActor(Actor):
    """Dispatch actor.

    This actor is responsible for handling dispatches for a microgrid.

    This means staying in sync with the API and scheduling
    dispatches as necessary.
    """

    # pylint: disable=too-many-arguments
    def __init__(
        self,
        microgrid_id: int,
        grpc_channel: grpc.aio.Channel,
        svc_addr: str,
        updated_dispatch_sender: Sender[DispatchEvent],
        ready_dispatch_sender: Sender[Dispatch],
        poll_interval: timedelta = _DEFAULT_POLL_INTERVAL,
    ) -> None:
        """Initialize the actor.

        Args:
            microgrid_id: The microgrid ID to handle dispatches for.
            grpc_channel: The gRPC channel to use for communication with the API.
            svc_addr: Address of the service to connect to.
            updated_dispatch_sender: A sender for new or updated dispatches.
            ready_dispatch_sender: A sender for ready dispatches.
            poll_interval: The interval to poll the API for dispatche changes.
        """
        super().__init__(name="dispatch")

        self._client = Client(grpc_channel, svc_addr)
        self._dispatches: dict[int, Dispatch] = {}
        self._scheduled: dict[int, asyncio.Task[None]] = {}
        self._microgrid_id = microgrid_id
        self._updated_dispatch_sender = updated_dispatch_sender
        self._ready_dispatch_sender = ready_dispatch_sender
        self._poll_interval = poll_interval

    async def _run(self) -> None:
        """Run the actor."""
        try:
            while True:
                await self._fetch()
                await asyncio.sleep(self._poll_interval.total_seconds())
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
            async for dispatch in self._client.list(microgrid_id=self._microgrid_id):
                self._dispatches[dispatch.id] = dispatch

                old_dispatch = old_dispatches.pop(dispatch.id, None)
                if not old_dispatch:
                    self._update_dispatch_schedule(dispatch, None)
                    _logger.info("New dispatch: %s", dispatch)
                    await self._updated_dispatch_sender.send(Created(dispatch=dispatch))
                elif dispatch.update_time != old_dispatch.update_time:
                    self._update_dispatch_schedule(dispatch, old_dispatch)
                    _logger.info("Updated dispatch: %s", dispatch)
                    await self._updated_dispatch_sender.send(Updated(dispatch=dispatch))

        except grpc.aio.AioRpcError as error:
            _logger.error("Error fetching dispatches: %s", error)
            self._dispatches = old_dispatches
            return

        for dispatch in old_dispatches.values():
            _logger.info("Deleted dispatch: %s", dispatch)
            await self._updated_dispatch_sender.send(Deleted(dispatch=dispatch))
            if task := self._scheduled.pop(dispatch.id, None):
                task.cancel()

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
            next_run = self.calculate_next_run(dispatch, now)

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
            await self._ready_dispatch_sender.send(dispatch)

        _logger.info("Dispatch finished: %s", dispatch)
        self._scheduled.pop(dispatch.id)

    @classmethod
    def calculate_next_run(cls, dispatch: Dispatch, _from: datetime) -> datetime | None:
        """Calculate the next run of a dispatch.

        Args:
            dispatch: The dispatch to calculate the next run for.
            _from: The time to calculate the next run from.

        Returns:
            The next run of the dispatch or None if the dispatch is finished.
        """
        if (
            not dispatch.recurrence.frequency
            or dispatch.recurrence.frequency == Frequency.UNSPECIFIED
        ):
            if _from > dispatch.start_time:
                return None
            return dispatch.start_time

        # Make sure no weekday is UNSPECIFIED
        if Weekday.UNSPECIFIED in dispatch.recurrence.byweekdays:
            _logger.warning(
                "Dispatch %s has UNSPECIFIED weekday, ignoring...", dispatch.id
            )
            return None

        count, until = (None, None)
        if end := dispatch.recurrence.end_criteria:
            count = end.count
            until = end.until

        next_run = rrule.rrule(
            freq=_RRULE_FREQ_MAP[dispatch.recurrence.frequency],
            dtstart=dispatch.start_time,
            count=count,
            until=until,
            byminute=dispatch.recurrence.byminutes,
            byhour=dispatch.recurrence.byhours,
            byweekday=[
                _RRULE_WEEKDAY_MAP[weekday]
                for weekday in dispatch.recurrence.byweekdays
            ],
            bymonthday=dispatch.recurrence.bymonthdays,
            bymonth=dispatch.recurrence.bymonths,
            interval=dispatch.recurrence.interval,
        )

        return cast(datetime | None, next_run.after(_from, inc=True))
