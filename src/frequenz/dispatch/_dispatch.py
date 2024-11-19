# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Dispatch type with support for next_run calculation."""

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Iterator

from frequenz.client.dispatch.types import Dispatch as BaseDispatch


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
    def started(self) -> bool:
        """Check if the dispatch is started.

        Returns:
            True if the dispatch is started, False otherwise.
        """
        if self.deleted:
            return False

        return super().started

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
