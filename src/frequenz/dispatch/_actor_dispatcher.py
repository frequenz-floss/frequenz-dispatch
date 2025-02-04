# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches."""

import asyncio
import logging
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from frequenz.channels import Broadcast, Receiver
from frequenz.client.dispatch.types import TargetComponents
from frequenz.sdk.actor import Actor, BackgroundService

from ._dispatch import Dispatch

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class DispatchInfo:
    """Event emitted when the dispatch changes."""

    components: TargetComponents
    """Components to be used."""

    dry_run: bool
    """Whether this is a dry run."""

    options: dict[str, Any]
    """Additional options."""


class ActorDispatcher(BackgroundService):
    """Helper class to manage actors based on dispatches.

    Example usage:

    ```python
    import os
    import asyncio
    from typing import override
    from frequenz.dispatch import Dispatcher, DispatchManagingActor, DispatchInfo
    from frequenz.client.dispatch.types import TargetComponents
    from frequenz.client.common.microgrid.components import ComponentCategory
    from frequenz.channels import Receiver, Broadcast, select, selected_from
    from frequenz.sdk.actor import Actor, run

    class MyActor(Actor):
        def __init__(
                self,
                *,
                name: str | None = None,
        ) -> None:
            super().__init__(name=name)
            self._dispatch_updates_receiver: Receiver[DispatchInfo] | None = None
            self._dry_run: bool = False
            self._options: dict[str, Any] = {}

        @classmethod
        def new_with_dispatch(
                cls,
                initial_dispatch: DispatchInfo,
                dispatch_updates_receiver: Receiver[DispatchInfo],
                *,
                name: str | None = None,
        ) -> "Self":
            self = cls(name=name)
            self._dispatch_updates_receiver = dispatch_updates_receiver
            self._update_dispatch_information(initial_dispatch)
            return self

        @override
        async def _run(self) -> None:
            other_recv: Receiver[Any] = ...

            if self._dispatch_updates_receiver is None:
                async for msg in other_recv:
                    # do stuff
                    ...
            else:
                await self._run_with_dispatch(other_recv)

        async def _run_with_dispatch(self, other_recv: Receiver[Any]) -> None:
            async for selected in select(self._dispatch_updates_receiver, other_recv):
                if selected_from(selected, self._dispatch_updates_receiver):
                    self._update_dispatch_information(selected.message)
                elif selected_from(selected, other_recv):
                    # do stuff
                    ...
                else:
                    assert False, f"Unexpected selected receiver: {selected}"

        def _update_dispatch_information(self, dispatch_update: DispatchInfo) -> None:
            print("Received update:", dispatch_update)
            self._dry_run = dispatch_update.dry_run
            self._options = dispatch_update.options
            match dispatch_update.components:
                case []:
                    print("Dispatch: Using all components")
                case list() as ids if isinstance(ids[0], int):
                    component_ids = ids
                case [ComponentCategory.BATTERY, *_]:
                    component_category = ComponentCategory.BATTERY
                case unsupported:
                    print(
                        "Dispatch: Requested an unsupported selector %r, "
                        "but only component IDs or category BATTERY are supported.",
                        unsupported,
                    )

    async def main():
        url = os.getenv("DISPATCH_API_URL", "grpc://fz-0004.frequenz.io:50051")
        key  = os.getenv("DISPATCH_API_KEY", "some-key")

        microgrid_id = 1

        dispatcher = Dispatcher(
            microgrid_id=microgrid_id,
            server_url=url,
            key=key
        )
        dispatcher.start()

        status_receiver = dispatcher.new_running_state_event_receiver("EXAMPLE_TYPE")

        managing_actor = DispatchManagingActor(
            actor_factory=MyActor.new_with_dispatch,
            running_status_receiver=status_receiver,
        )

        await run(managing_actor)
    ```
    """

    def __init__(
        self,
        actor_factory: Callable[[DispatchInfo, Receiver[DispatchInfo]], Actor],
        running_status_receiver: Receiver[Dispatch],
    ) -> None:
        """Initialize the dispatch handler.

        Args:
            actor_factory: A callable that creates an actor with some initial dispatch
                information.
            running_status_receiver: The receiver for dispatch running status changes.
        """
        super().__init__()
        self._dispatch_rx = running_status_receiver
        self._actor_factory = actor_factory
        self._actor: Actor | None = None
        self._updates_channel = Broadcast[DispatchInfo](
            name="dispatch_updates_channel", resend_latest=True
        )
        self._updates_sender = self._updates_channel.new_sender()

    def start(self) -> None:
        """Start the background service."""
        self._tasks.add(asyncio.create_task(self._run()))

    async def _start_actor(self, dispatch: Dispatch) -> None:
        """Start all actors."""
        dispatch_update = DispatchInfo(
            components=dispatch.target,
            dry_run=dispatch.dry_run,
            options=dispatch.payload,
        )

        if self._actor:
            sent_str = ""
            if self._updates_sender is not None:
                sent_str = ", sent a dispatch update instead of creating a new actor"
                await self._updates_sender.send(dispatch_update)
            _logger.warning(
                "Actor for dispatch type %r is already running%s",
                dispatch.type,
                sent_str,
            )
        else:
            _logger.info("Starting actor for dispatch type %r", dispatch.type)
            self._actor = self._actor_factory(
                dispatch_update, self._updates_channel.new_receiver()
            )
            self._actor.start()

    async def _stop_actor(self, stopping_dispatch: Dispatch, msg: str) -> None:
        """Stop all actors.

        Args:
            stopping_dispatch: The dispatch that is stopping the actor.
            msg: The message to be passed to the actors being stopped.
        """
        if self._actor is None:
            _logger.warning(
                "Actor for dispatch type %r is not running", stopping_dispatch.type
            )
        else:
            await self._actor.stop(msg)
            self._actor = None

    async def _run(self) -> None:
        """Wait for dispatches and handle them."""
        async for dispatch in self._dispatch_rx:
            await self._handle_dispatch(dispatch=dispatch)

    async def _handle_dispatch(self, dispatch: Dispatch) -> None:
        """Handle a dispatch.

        Args:
            dispatch: The dispatch to handle.
        """
        if dispatch.started:
            await self._start_actor(dispatch)
        else:
            await self._stop_actor(dispatch, "Dispatch stopped")
