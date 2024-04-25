# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A highlevel interface for the dispatch API."""

import abc
from typing import Protocol, TypeVar

import grpc.aio
from frequenz.channels import Broadcast, Receiver
from frequenz.client.dispatch.types import Dispatch

from frequenz.dispatch._event import DispatchEvent
from frequenz.dispatch.actor import DispatchingActor

ReceivedT_co = TypeVar("ReceivedT_co", covariant=True)
"""The type being received."""


class ReceiverFetcher(Protocol[ReceivedT_co]):
    """An interface that just exposes a `new_receiver` method."""

    @abc.abstractmethod
    def new_receiver(
        self, *, name: str | None = None, limit: int = 50
    ) -> Receiver[ReceivedT_co]:
        """Get a receiver from the channel.

        Args:
            name: A name to identify the receiver in the logs.
            limit: The maximum size of the receiver.

        Returns:
            A receiver instance.
        """


class Dispatcher:
    """A highlevel interface for the dispatch API.

    This class provides a highlevel interface to the dispatch API.
    It provides two channels:

    One that sends a dispatch event message whenever a dispatch is created, updated or deleted.

    The other sends a dispatch message whenever a dispatch is ready to be
    executed according to the schedule.

    allows to receive new dispatches and ready dispatches.

    Example: Processing ready-to-execute dispatches
        ```python
        import grpc.aio

        async def run():
            grpc_channel = grpc.aio.insecure_channel("localhost:50051")
            microgrid_id = 1
            service_address = "localhost:50051"

            dispatcher = Dispatcher(microgrid_id, grpc_channel, service_address)
            dispatcher.start()  # this will start the actor

            ready_receiver = dispatcher.ready_to_execute.new_receiver()

            async for dispatch in ready_receiver:
                print(f"Executing dispatch {dispatch.id}, due on {dispatch.start_time}")
                # execute the dispatch
        ```

    Example: Getting notification about dispatch lifecycle events
        ```python
        from typing import assert_never

        import grpc.aio
        from frequenz.dispatch import Created, Deleted, Dispatcher, Updated


        async def run():
            grpc_channel = grpc.aio.insecure_channel("localhost:50051")
            microgrid_id = 1
            service_address = "localhost:50051"
            dispatcher = Dispatcher(microgrid_id, grpc_channel, service_address)
            dispatcher.start()  # this will start the actor

            events_receiver = dispatcher.lifecycle_events.new_receiver()

            async for event in events_receiver:
                match event:
                    case Created(dispatch):
                        print(f"A dispatch was created: {dispatch}")
                    case Deleted(dispatch):
                        print(f"A dispatch was deleted: {dispatch}")
                    case Updated(dispatch):
                        print(f"A dispatch was updated: {dispatch}")
                    case _ as unhandled:
                        assert_never(unhandled)
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
        self._ready_channel = Broadcast[Dispatch](name="ready_dispatches")
        self._updated_channel = Broadcast[DispatchEvent](name="new_dispatches")
        self._actor = DispatchingActor(
            microgrid_id,
            grpc_channel,
            svc_addr,
            self._updated_channel.new_sender(),
            self._ready_channel.new_sender(),
        )

    async def start(self) -> None:
        """Start the actor."""
        self._actor.start()

    @property
    def lifecycle_events(self) -> ReceiverFetcher[DispatchEvent]:
        """Return new, updated or deleted dispatches receiver.

        Returns:
            A new receiver for new dispatches.
        """
        return self._updated_channel

    @property
    def ready_to_execute(self) -> ReceiverFetcher[Dispatch]:
        """Return ready dispatches receiver.

        Returns:
            A new receiver for ready dispatches.
        """
        return self._ready_channel
