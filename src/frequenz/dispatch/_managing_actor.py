# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches."""

import logging
from dataclasses import dataclass
from typing import Any, Set

from frequenz.channels import Receiver, Sender
from frequenz.client.dispatch.types import TargetComponents
from frequenz.sdk.actor import Actor

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
    from frequenz.dispatch import Dispatcher, DispatchManagingActor, DispatchUpdate
    from frequenz.client.dispatch.types import TargetComponents
    from frequenz.client.common.microgrid.components import ComponentCategory

    from frequenz.channels import Receiver, Broadcast

    class MyActor(Actor):
        def __init__(self, updates_channel: Receiver[DispatchUpdate]):
            super().__init__()
            self._updates_channel = updates_channel
            self._dry_run: bool
            self._options : dict[str, Any]

        async def _run(self) -> None:
            while True:
                update = await self._updates_channel.receive()
                print("Received update:", update)

                self.set_components(update.components)
                self._dry_run = update.dry_run
                self._options = update.options

        def set_components(self, components: TargetComponents) -> None:
            match components:
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

    async def run():
        url = os.getenv("DISPATCH_API_URL", "grpc://fz-0004.frequenz.io:50051")
        key  = os.getenv("DISPATCH_API_KEY", "some-key")

        microgrid_id = 1

        dispatcher = Dispatcher(
            microgrid_id=microgrid_id,
            server_url=url,
            key=key
        )

        # Create update channel to receive dispatch update events pre-start and mid-run
        dispatch_updates_channel = Broadcast[DispatchUpdate](name="dispatch_updates_channel")

        # Start actor and give it an dispatch updates channel receiver
        my_actor = MyActor(dispatch_updates_channel.new_receiver())

        status_receiver = dispatcher.new_running_state_event_receiver("EXAMPLE_TYPE")

        managing_actor = DispatchManagingActor(
            actor=my_actor,
            running_status_receiver=status_receiver,
            updates_sender=dispatch_updates_channel.new_sender(),
        )

        await asyncio.gather(dispatcher.start(), managing_actor.start())
    ```
    """

    def __init__(
        self,
        actor: Actor | Set[Actor],
        running_status_receiver: Receiver[Dispatch],
        updates_sender: Sender[DispatchUpdate] | None = None,
    ) -> None:
        """Initialize the dispatch handler.

        Args:
            actor: A set of actors or a single actor to manage.
            running_status_receiver: The receiver for dispatch running status changes.
            updates_sender: The sender for dispatch events
        """
        super().__init__()
        self._dispatch_rx = running_status_receiver
        self._actors: frozenset[Actor] = frozenset(
            [actor] if isinstance(actor, Actor) else actor
        )
        self._updates_sender = updates_sender

    def _start_actors(self) -> None:
        """Start all actors."""
        for actor in self._actors:
            if actor.is_running:
                _logger.warning("Actor %s is already running", actor.name)
            else:
                actor.start()

    async def _stop_actors(self, msg: str) -> None:
        """Stop all actors.

        Args:
            msg: The message to be passed to the actors being stopped.
        """
        for actor in self._actors:
            if actor.is_running:
                await actor.stop(msg)
            else:
                _logger.warning("Actor %s is not running", actor.name)

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
            if self._updates_sender is not None:
                _logger.info("Updated by dispatch %s", dispatch.id)
                await self._updates_sender.send(
                    DispatchUpdate(
                        components=dispatch.target,
                        dry_run=dispatch.dry_run,
                        options=dispatch.payload,
                    )
                )

            _logger.info("Started by dispatch %s", dispatch.id)
            self._start_actors()
        else:
            _logger.info("Stopped by dispatch %s", dispatch.id)
            await self._stop_actors("Dispatch stopped")
