# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches."""

import logging
from abc import abstractmethod
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from frequenz.channels import Receiver, Sender
from frequenz.client.dispatch.types import TargetComponents
from frequenz.sdk.actor import Actor
from typing_extensions import override

from ._dispatch import Dispatch

_logger = logging.getLogger(__name__)


@dataclass(frozen=True, kw_only=True)
class DispatchUpdate:
    """Event emitted when the dispatch changes."""

    components: TargetComponents
    """Components to be used."""

    dry_run: bool
    """Whether this is a dry run."""

    options: dict[str, Any]
    """Additional options."""


class DispatchManagingActor(Actor):
    """Helper class to manage actors based on dispatches.

    Example usage:

    ```python
    import os
    import asyncio
    from typing import override
    from frequenz.dispatch import Dispatcher, DispatchManagingActor, DispatchUpdate, DispatchableActor
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
            self._dispatch_updates_receiver: Receiver[DispatchUpdate] | None = None
            self._dry_run: bool = False
            self._options: dict[str, Any] = {}

        @classmethod
        def new_with_dispatch(
                cls,
                initial_dispatch: DispatchUpdate,
                dispatch_updates_receiver: Receiver[DispatchUpdate],
                *,
                name: str | None = None,
        ) -> Self:
            self = cls(name=name)
            self._dispatch_updates_receiver = dispatch_updates_receiver
            self._update_dispatch_information(initial_dispatch)
            return self

        @override
        async def _run(self) -> None:
            other_recv: Receiver[Any] = ...

            if self._dispatch_updates_receiver is None:
                async for msg in other:
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

        def _update_dispatch_information(self, dispatch_update: DispatchUpdate) -> None:
            print("Received update:", dispatch_update)
            self._dry_run = dispatch_update.dry_run
            self._options = dispatch_update.options
            match components:
                case [int(), *_] as component_ids:
                    print("Dispatch: Setting components to %s", components)
                case [ComponentCategory.BATTERY, *_]:
                    print("Dispatch: Using all battery components")
                case unsupported:
                    print(
                        "Dispatch: Requested an unsupported target component %r, "
                        "but only component IDs or category BATTERY are supported.",
                        unsupported,
                    )

    async def run():
        url = os.getenv("DISPATCH_API_URL", "grpc://fz-0004.frequenz.io:50051")
        key  = os.getenv("DISPATCH_API_KEY", "some-key")

        microgrid_id = 1

        dispatcher = Dispatcher(
            microgrid_id=microgrid_id,
            server_url=url,
            key=key
        )
        dispatcher.start()

        # Create update channel to receive dispatch update events pre-start and mid-run
        dispatch_updates_channel = Broadcast[DispatchUpdate](name="dispatch_updates_channel")

        # Start actor and give it an dispatch updates channel receiver
        my_actor = MyActor(dispatch_updates_channel.new_receiver())

        status_receiver = dispatcher.running_status_change.new_receiver()

        managing_actor = DispatchManagingActor(
            actor_factory=labda initial_dispatch: MyActor.new_with_dispatch(
                initial_dispatch, dispatch_updates_channel.new_receiver(),
            ),
            dispatch_type="EXAMPLE",
            running_status_receiver=status_receiver,
            updates_sender=dispatch_updates_channel.new_sender(),
        )

        await run(managing_actor)
    ```
    """

    def __init__(
        self,
        actor_factory: Callable[[DispatchUpdate], Actor],
        dispatch_type: str,
        running_status_receiver: Receiver[Dispatch],
        updates_sender: Sender[DispatchUpdate] | None = None,
    ) -> None:
        """Initialize the dispatch handler.

        Args:
            actor_factory: A callable that creates an actor with some initial dispatch
                information.
            dispatch_type: The type of dispatches to handle.
            running_status_receiver: The receiver for dispatch running status changes.
            updates_sender: The sender for dispatch events
        """
        super().__init__()
        self._dispatch_rx = running_status_receiver
        self._actor_factory = actor_factory
        self._actor: Actor | None = None
        self._dispatch_type = dispatch_type
        self._updates_sender = updates_sender

    async def _start_actor(self, dispatch_update: DispatchUpdate) -> None:
        """Start all actors."""
        if self._actor is None:
            sent_str = ""
            if self._updates_sender is not None:
                sent_str = ", sent a dispatch update instead of creating a new actor"
                await self._updates_sender.send(dispatch_update)
            _logger.warning(
                "Actor for dispatch type %r is already running%s",
                self._dispatch_type,
                sent_str,
            )
        else:
            self._actor = self._actor_factory(dispatch_update)
            self._actor.start()

    async def _stop_actor(self, msg: str) -> None:
        """Stop all actors.

        Args:
            msg: The message to be passed to the actors being stopped.
        """
        if self._actor is None:
            _logger.warning(
                "Actor for dispatch type %r is not running", self._dispatch_type
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
        if dispatch.type != self._dispatch_type:
            _logger.debug(
                "Ignoring dispatch %s, handled type is %r but received %r",
                dispatch.id,
                self._dispatch_type,
                dispatch.type,
            )
            return

        if dispatch.started:
            dispatch_update = DispatchUpdate(
                components=dispatch.target,
                dry_run=dispatch.dry_run,
                options=dispatch.payload,
            )
            if self._actor is None:
                _logger.info(
                    "A new dispatch with ID %s became active for type %r and the "
                    "actor was not running, starting...",
                    dispatch.id,
                    self._dispatch_type,
                )
                self._actor = self._actor_factory(dispatch_update)
            elif self._updates_sender is not None:
                _logger.info(
                    "A new dispatch with ID %s became active for type %r and the "
                    "actor was running, sending update...",
                    dispatch.id,
                    self._dispatch_type,
                )
                await self._updates_sender.send(dispatch_update)
        else:
            _logger.info(
                "Actor for dispatch type %r stopped by dispatch ID %s",
                self._dispatch_type,
                dispatch.id,
            )
            await self._stop_actor("Dispatch stopped")
