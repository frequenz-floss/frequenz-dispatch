# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A highlevel interface for the dispatch API."""

import grpc.aio
from frequenz.channels import Broadcast, Receiver
from frequenz.client.dispatch.types import Dispatch

from frequenz.dispatch.actor import DispatchActor, DispatchEvent

__all__ = ["Dispatcher"]


class Dispatcher:
    """A highlevel interface for the dispatch API.

    This class provides a highlevel interface to the dispatch API. It
    allows to receive new dispatches and ready dispatches.

    Example:
        ```python
        from frequenz.dispatch import Dispatcher

        async def run():
            dispatcher = Dispatcher(API_CONNECTION_INFO)
            dispatcher.start()  # this will start the actor
            dispatch_arrived = dispatcher.new_dispatches().new_receiver()
            dispatch_ready = dispatcher.ready_dispatches().new_receiver()
        ```
    """

    def __init__(
        self, microgrid_id: int, grpc_channel: grpc.aio.Channel, svc_addr: str
    ):
        """Initialize the dispatcher.

        Args:
            microgrid_id: The microgrid id.
            grpc_channel: The gRPC channel.
            svc_addr: The service address.
        """
        self._ready_channel = Broadcast[Dispatch]("ready_dispatches")
        self._updated_channel = Broadcast[DispatchEvent]("new_dispatches")
        self._actor = DispatchActor(
            microgrid_id,
            grpc_channel,
            svc_addr,
            self._updated_channel.new_sender(),
            self._ready_channel.new_sender(),
        )

    async def start(self) -> None:
        """Start the actor."""
        self._actor.start()

    def updated_dispatches(self) -> Receiver[DispatchEvent]:
        """Return new, updated or deleted dispatches receiver.

        Returns:
            A new receiver for new dispatches.
        """
        return self._updated_channel.new_receiver()

    def ready_dispatches(self) -> Receiver[Dispatch]:
        """Return ready dispatches receiver.

        Returns:
            A new receiver for ready dispatches.
        """
        return self._ready_channel.new_receiver()
