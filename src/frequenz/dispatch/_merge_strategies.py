# License: MIT
# Copyright © 2025 Frequenz Energy-as-a-Service GmbH

"""Different merge strategies for dispatch running state events."""

import logging
from abc import abstractmethod
from collections.abc import Mapping

from typing_extensions import override

from ._bg_service import MergeStrategy
from ._dispatch import Dispatch


class MergeByIdentity(MergeStrategy):
    """Merge running intervals based on a dispatch configuration."""

    @abstractmethod
    def identity(self, dispatch: Dispatch) -> int:
        """Identity function for the merge criteria."""

    @override
    def filter(self, dispatches: Mapping[int, Dispatch], dispatch: Dispatch) -> bool:
        """Filter dispatches based on the merge strategy.

        Keeps start events.
        Keeps stop events only if no other dispatches matching the
        strategy's criteria are running.
        """
        if dispatch.started:
            logging.debug("Keeping start event %s", dispatch.id)
            return True

        other_dispatches_running = any(
            existing_dispatch.started
            for existing_dispatch in dispatches.values()
            if (
                self.identity(existing_dispatch) == self.identity(dispatch)
                and existing_dispatch.id != dispatch.id
            )
        )

        logging.debug(
            "stop event %s because other_dispatches_running=%s",
            dispatch.id,
            other_dispatches_running,
        )
        return not other_dispatches_running


class MergeByType(MergeByIdentity):
    """Merge running intervals based on the dispatch type."""

    @override
    def identity(self, dispatch: Dispatch) -> int:
        """Identity function for the merge criteria."""
        return hash(dispatch.type)


class MergeByTypeTarget(MergeByType):
    """Merge running intervals based on the dispatch type and target."""

    @override
    def identity(self, dispatch: Dispatch) -> int:
        """Identity function for the merge criteria."""
        return hash((dispatch.type, tuple(dispatch.target)))
