# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A highlevel interface for the dispatch API."""

import abc
from typing import Protocol, TypeVar

import grpc.aio
from frequenz.channels import Broadcast, Receiver

from ._dispatch import Dispatch
from ._event import DispatchEvent
from .actor import DispatchingActor

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
    executed according to the schedule or the running status of the dispatch
    changed in a way that could potentially require the actor to start, stop or
    reconfigure itself.

    Example: Processing running state change dispatches
        ```python
        import grpc.aio
        from unittest.mock import MagicMock

        async def run():
            grpc_channel = grpc.aio.insecure_channel("localhost:50051")
            microgrid_id = 1
            service_address = "localhost:50051"

            dispatcher = Dispatcher(microgrid_id, grpc_channel, service_address)
            actor = MagicMock() # replace with your actor

            changed_running_status_rx = dispatcher.running_status_change.new_receiver()

            async for dispatch in changed_running_status_rx:
                print(f"Executing dispatch {dispatch.id}, due on {dispatch.start_time}")
                if dispatch.running:
                    if actor.is_running:
                        actor.reconfigure(
                            components=dispatch.selector,
                            run_parameters=dispatch.payload
                        )  # this will reconfigure the actor
                    else:
                        # this will start the actor
                        # and run it for the duration of the dispatch
                        actor.start(duration=dispatch.duration, dry_run=dispatch.dry_run)
                else:
                    actor.stop()  # this will stop the actor
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
        self._running_state_channel = Broadcast[Dispatch](name="running_state_change")
        self._lifecycle_events_channel = Broadcast[DispatchEvent](
            name="lifecycle_events"
        )
        self._actor = DispatchingActor(
            microgrid_id,
            grpc_channel,
            svc_addr,
            self._lifecycle_events_channel.new_sender(),
            self._running_state_channel.new_sender(),
        )

    async def start(self) -> None:
        """Start the actor."""
        self._actor.start()

    @property
    def lifecycle_events(self) -> ReceiverFetcher[DispatchEvent]:
        """Return new, updated or deleted dispatches receiver fetcher.

        Returns:
            A new receiver for new dispatches.
        """
        return self._lifecycle_events_channel

    @property
    def running_status_change(self) -> ReceiverFetcher[Dispatch]:
        """Return running status change receiver fetcher.

        This receiver will receive a message whenever the current running
        status of a dispatch changes.

        Usually, one message per scheduled run is to be expected.
        However, things get complicated when a dispatch was modified:

        If it was currently running and the modification now says
        it should not be running or running with different parameters,
        then a message will be sent.

        In other words: Any change that is expected to make an actor start, stop
        or reconfigure itself with new parameters causes a message to be
        sent.

        A non-exhaustive list of possible changes that will cause a message to be sent:
         - The normal scheduled start_time has been reached
         - The duration of the dispatch has been modified
         - The start_time has been modified to be in the future
         - The component selection changed
         - The active status changed
         - The dry_run status changed
         - The payload changed
         - The dispatch was deleted

        Note: Reaching the end time (start_time + duration) will not
        send a message, except when it was reached by modifying the duration.


        Returns:
            A new receiver for dispatches whose running status changed.
        """
        return self._running_state_channel
