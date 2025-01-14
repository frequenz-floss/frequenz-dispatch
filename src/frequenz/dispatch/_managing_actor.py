# License: All rights reserved
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Helper class to manage actors based on dispatches."""

import logging
from dataclasses import dataclass
from typing import Any, Set

from frequenz.channels import Broadcast, Receiver
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

        # Start actor and give it an dispatch updates channel receiver
        my_actor = MyActor(dispatch_updates_channel.new_receiver())

        status_receiver = dispatcher.running_status_change.new_receiver()

        managing_actor = DispatchManagingActor(
            actor=my_actor,
            dispatch_type="EXAMPLE",
            running_status_receiver=status_receiver,
            updates_sender=dispatch_updates_channel.new_sender(),
        )

        await asyncio.gather(dispatcher.start(), managing_actor.start())
    ```
    """

    def __init__(
        self,
        running_status_receiver: Receiver[Dispatch],
        actor: Actor | Set[Actor] | None = None,
    ) -> None:
        """Initialize the dispatch handler.

        Args:
            running_status_receiver: The receiver for dispatch running status changes.
            actor: Optional, but should be set later in set_actor(). A set of
                actors or a single actor to manage.
        """
        super().__init__()
        self._dispatch_rx = running_status_receiver
        self._actors: frozenset[Actor] = (
            frozenset()
            if actor is None
            else frozenset([actor] if isinstance(actor, Actor) else actor)
        )
        self._updates_channel = Broadcast[DispatchUpdate](
            name="dispatch_updates_channel", resend_latest=True
        )
        self._updates_sender = self._updates_channel.new_sender()

    def set_actor(self, actor: Actor | Set[Actor]) -> None:
        """Set the actor to manage.

        Args:
            actor: A set of actors or a single actor to manage.
        """
        self._actors = (
            frozenset([actor]) if isinstance(actor, Actor) else frozenset(actor)
        )

    def new_receiver(self) -> Receiver[DispatchUpdate]:
        """Create a new receiver for dispatch updates.

        Returns:
            A new receiver for dispatch updates.
        """
        return self._updates_channel.new_receiver()

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
