# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Dispatch type with support for next_run calculation."""


import logging
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from typing import Iterator, cast

from dateutil import rrule
from frequenz.client.dispatch.types import Dispatch as BaseDispatch
from frequenz.client.dispatch.types import Frequency, Weekday

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


class RunningState(Enum):
    """The running state of a dispatch."""

    RUNNING = "RUNNING"
    """The dispatch is running."""

    STOPPED = "STOPPED"
    """The dispatch is stopped."""

    DIFFERENT_TYPE = "DIFFERENT_TYPE"
    """The dispatch is for a different type."""


@dataclass(frozen=True)
class Dispatch(BaseDispatch):
    """Dispatch type with extra functionality."""

    deleted: bool = False
    """Whether the dispatch is deleted."""

    running_state_change_synced: datetime | None = None
    """The last time a message was sent about the running state change."""

    def __init__(
        self,
        client_dispatch: BaseDispatch,
        deleted: bool = False,
        running_state_change_synced: datetime | None = None,
    ):
        """Initialize the dispatch.

        Args:
            client_dispatch: The client dispatch.
            deleted: Whether the dispatch is deleted.
            running_state_change_synced: Timestamp of the last running state change message.
        """
        super().__init__(**client_dispatch.__dict__)
        # Work around frozen to set deleted
        object.__setattr__(self, "deleted", deleted)
        object.__setattr__(
            self,
            "running_state_change_synced",
            running_state_change_synced,
        )

    def _set_deleted(self) -> None:
        """Mark the dispatch as deleted."""
        object.__setattr__(self, "deleted", True)

    @property
    def _running_status_notified(self) -> bool:
        """Check that the latest running state change notification was sent.

        Returns:
            True if the latest running state change notification was sent, False otherwise.
        """
        return self.running_state_change_synced == self.update_time

    def _set_running_status_notified(self) -> None:
        """Mark the latest running state change notification as sent."""
        object.__setattr__(self, "running_state_change_synced", self.update_time)

    def running(self, type_: str) -> RunningState:
        """Check if the dispatch is currently supposed to be running.

        Args:
            type_: The type of the dispatch that should be running.

        Returns:
            RUNNING if the dispatch is running,
            STOPPED if it is stopped,
            DIFFERENT_TYPE if it is for a different type.
        """
        if self.type != type_:
            return RunningState.DIFFERENT_TYPE

        if not self.active or self.deleted:
            return RunningState.STOPPED

        now = datetime.now(tz=timezone.utc)
        if until := self._until(now):
            return RunningState.RUNNING if now < until else RunningState.STOPPED

        return RunningState.STOPPED

    @property
    def until(self) -> datetime | None:
        """Time when the dispatch should end.

        Returns the time that a running dispatch should end.
        If the dispatch is not running, None is returned.

        Returns:
            The time when the dispatch should end or None if the dispatch is not running.
        """
        if not self.active or self.deleted:
            return None

        now = datetime.now(tz=timezone.utc)
        return self._until(now)

    @property
    # noqa is needed because of a bug in pydoclint that makes it think a `return` without a return
    # value needs documenting
    def missed_runs(self) -> Iterator[datetime]:  # noqa: DOC405
        """Yield all missed runs of a dispatch.

        Yields all missed runs of a dispatch.

        If a running state change notification was not sent in time
        due to connection issues, this method will yield all missed runs
        since the last sent notification.

        Returns:
            A generator that yields all missed runs of a dispatch.
        """
        if self.update_time == self.running_state_change_synced:
            return

        from_time = self.update_time
        now = datetime.now(tz=timezone.utc)

        while (next_run := self.next_run_after(from_time)) and next_run < now:
            yield next_run
            from_time = next_run

    @property
    def next_run(self) -> datetime | None:
        """Calculate the next run of a dispatch.

        Returns:
            The next run of the dispatch or None if the dispatch is finished.
        """
        return self.next_run_after(datetime.now(tz=timezone.utc))

    def next_run_after(self, after: datetime) -> datetime | None:
        """Calculate the next run of a dispatch.

        Args:
            after: The time to calculate the next run from.

        Returns:
            The next run of the dispatch or None if the dispatch is finished.
        """
        if (
            not self.recurrence.frequency
            or self.recurrence.frequency == Frequency.UNSPECIFIED
        ):
            if after > self.start_time:
                return None
            return self.start_time

        # Make sure no weekday is UNSPECIFIED
        if Weekday.UNSPECIFIED in self.recurrence.byweekdays:
            _logger.warning("Dispatch %s has UNSPECIFIED weekday, ignoring...", self.id)
            return None

        # No type information for rrule, so we need to cast
        return cast(datetime | None, self._prepare_rrule().after(after, inc=True))

    def _prepare_rrule(self) -> rrule.rrule:
        """Prepare the rrule object.

        Returns:
            The rrule object.
        """
        count, until = (None, None)
        if end := self.recurrence.end_criteria:
            count = end.count
            until = end.until

        rrule_obj = rrule.rrule(
            freq=_RRULE_FREQ_MAP[self.recurrence.frequency],
            dtstart=self.start_time,
            count=count,
            until=until,
            byminute=self.recurrence.byminutes,
            byhour=self.recurrence.byhours,
            byweekday=[
                _RRULE_WEEKDAY_MAP[weekday] for weekday in self.recurrence.byweekdays
            ],
            bymonthday=self.recurrence.bymonthdays,
            bymonth=self.recurrence.bymonths,
            interval=self.recurrence.interval,
        )

        return rrule_obj

    def _until(self, now: datetime) -> datetime | None:
        """Calculate the time when the dispatch should end.

        If no previous run is found, None is returned.

        Args:
            now: The current time.

        Returns:
            The time when the dispatch should end or None if the dispatch is not running.
        """
        if (
            not self.recurrence.frequency
            or self.recurrence.frequency == Frequency.UNSPECIFIED
        ):
            return self.start_time + self.duration

        latest_past_start: datetime | None = self._prepare_rrule().before(now, inc=True)

        if not latest_past_start:
            return None

        return latest_past_start + self.duration
