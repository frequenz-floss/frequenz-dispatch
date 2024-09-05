# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A highlevel interface for the dispatch API."""

import abc
from typing import Protocol, TypeVar

from frequenz.channels import Broadcast, Receiver
from frequenz.client.dispatch import Client

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

    Lifecycle events:
        A channel that sends a dispatch event message whenever a dispatch
        is created, updated or deleted.

    Running status change:
        Sends a dispatch message whenever a dispatch is ready
        to be executed according to the schedule or the running status of the
        dispatch changed in a way that could potentially require the consumer to start,
        stop or reconfigure itself.

    Example: Processing running state change dispatches
        ```python
        import os
        from frequenz.dispatch import Dispatcher, RunningState
        from unittest.mock import MagicMock

        async def run():
            url = os.getenv("DISPATCH_API_URL", "grpc://fz-0004.frequenz.io:50051")
            key  = os.getenv("DISPATCH_API_KEY", "some-key")

            microgrid_id = 1

            dispatcher = Dispatcher(
                microgrid_id=microgrid_id,
                server_url=url,
                key=key
            )
            await dispatcher.start()

            actor = MagicMock() # replace with your actor

            changed_running_status = dispatcher.running_status_change.new_receiver()

            async for dispatch in changed_running_status:
                match dispatch.running("DEMO_TYPE"):
                    case RunningState.RUNNING:
                        print(f"Executing dispatch {dispatch.id}, due on {dispatch.start_time}")
                        if actor.is_running:
                            actor.reconfigure(
                                components=dispatch.selector,
                                run_parameters=dispatch.payload, # custom actor parameters
                                dry_run=dispatch.dry_run,
                                until=dispatch.until,
                            )  # this will reconfigure the actor
                        else:
                            # this will start a new actor with the given components
                            # and run it for the duration of the dispatch
                            actor.start(
                                components=dispatch.selector,
                                run_parameters=dispatch.payload, # custom actor parameters
                                dry_run=dispatch.dry_run,
                                until=dispatch.until,
                            )
                    case RunningState.STOPPED:
                        actor.stop()  # this will stop the actor
                    case RunningState.DIFFERENT_TYPE:
                        pass  # dispatch not for this type
        ```

    Example: Getting notification about dispatch lifecycle events
        ```python
        import os
        from typing import assert_never

        from frequenz.dispatch import Created, Deleted, Dispatcher, Updated

        async def run():
            url = os.getenv("DISPATCH_API_URL", "grpc://fz-0004.frequenz.io:50051")
            key  = os.getenv("DISPATCH_API_KEY", "some-key")

            microgrid_id = 1

            dispatcher = Dispatcher(
                microgrid_id=microgrid_id,
                server_url=url,
                key=key
            )
            await dispatcher.start()  # this will start the actor

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

    Example: Creating a new dispatch and then modifying it.
        Note that this uses the lower-level `Client` class to create and update the dispatch.

        ```python
        import os
        from datetime import datetime, timedelta, timezone

        from frequenz.client.common.microgrid.components import ComponentCategory

        from frequenz.dispatch import Dispatcher

        async def run():
            url = os.getenv("DISPATCH_API_URL", "grpc://fz-0004.frequenz.io:50051")
            key  = os.getenv("DISPATCH_API_KEY", "some-key")

            microgrid_id = 1

            dispatcher = Dispatcher(
                microgrid_id=microgrid_id,
                server_url=url,
                key=key
            )
            await dispatcher.start()  # this will start the actor

            # Create a new dispatch
            new_dispatch = await dispatcher.client.create(
                microgrid_id=microgrid_id,
                type="ECHO_FREQUENCY",  # replace with your own type
                start_time=datetime.now(tz=timezone.utc) + timedelta(minutes=10),
                duration=timedelta(minutes=5),
                selector=ComponentCategory.INVERTER,
                payload={"font": "Times New Roman"},  # Arbitrary payload data
            )

            # Modify the dispatch
            await dispatcher.client.update(
                microgrid_id=microgrid_id,
                dispatch_id=new_dispatch.id,
                new_fields={"duration": timedelta(minutes=10)}
            )

            # Validate the modification
            modified_dispatch = await dispatcher.client.get(
                microgrid_id=microgrid_id, dispatch_id=new_dispatch.id
            )
            assert modified_dispatch.duration == timedelta(minutes=10)
        ```
    """

    def __init__(
        self,
        *,
        microgrid_id: int,
        server_url: str,
        key: str,
    ):
        """Initialize the dispatcher.

        Args:
            microgrid_id: The microgrid id.
            server_url: The URL of the dispatch service.
            key: The key to access the service.
        """
        self._running_state_channel = Broadcast[Dispatch](name="running_state_change")
        self._lifecycle_events_channel = Broadcast[DispatchEvent](
            name="lifecycle_events"
        )
        self._client = Client(server_url=server_url, key=key)
        self._actor = DispatchingActor(
            microgrid_id,
            self._client,
            self._lifecycle_events_channel.new_sender(),
            self._running_state_channel.new_sender(),
        )

    async def start(self) -> None:
        """Start the actor."""
        self._actor.start()

    @property
    def client(self) -> Client:
        """Return the client."""
        return self._client

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
