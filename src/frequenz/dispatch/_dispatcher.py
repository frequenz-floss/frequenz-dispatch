# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A highlevel interface for the dispatch API."""


import logging
from typing import Callable

from frequenz.channels import Receiver
from frequenz.client.dispatch import Client
from frequenz.sdk.actor import Actor

from ._actor_dispatcher import ActorDispatcher, DispatchInfo
from ._bg_service import DispatchScheduler, MergeStrategy
from ._dispatch import Dispatch
from ._event import DispatchEvent
from ._merge_strategies import MergeByIdentity

_logger = logging.getLogger(__name__)


class Dispatcher:
    """A highlevel interface for the dispatch API.

    This class provides a highlevel interface to the dispatch API.
    It provides two receiver functions:

    * [Lifecycle events receiver][frequenz.dispatch.Dispatcher.new_lifecycle_events_receiver]:
        Receives an event whenever a dispatch is created, updated or deleted.
    * [Running status change
        receiver][frequenz.dispatch.Dispatcher.new_running_state_event_receiver]:
        Receives an event whenever the running status of a dispatch changes.
        The running status of a dispatch can change due to a variety of reasons,
        such as but not limited to the dispatch being started, stopped, modified
        or deleted or reaching its scheduled start or end time.

        Any change that could potentially require the consumer to start, stop or
        reconfigure itself will cause a message to be sent.

    Example: Processing running state change dispatches
        ```python
        import os
        from frequenz.dispatch import Dispatcher
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

            changed_running_status = dispatcher.new_running_state_event_receiver("DISPATCH_TYPE")

            async for dispatch in changed_running_status:
                if dispatch.started:
                    print(f"Executing dispatch {dispatch.id}, due on {dispatch.start_time}")
                    if actor.is_running:
                        actor.reconfigure(
                            components=dispatch.target,
                            run_parameters=dispatch.payload, # custom actor parameters
                            dry_run=dispatch.dry_run,
                            until=dispatch.until,
                        )  # this will reconfigure the actor
                    else:
                        # this will start a new actor with the given components
                        # and run it for the duration of the dispatch
                        actor.start(
                            components=dispatch.target,
                            run_parameters=dispatch.payload, # custom actor parameters
                            dry_run=dispatch.dry_run,
                            until=dispatch.until,
                        )
                else:
                    actor.stop()  # this will stop the actor
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

            events_receiver = dispatcher.new_lifecycle_events_receiver("DISPATCH_TYPE")

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
                target=ComponentCategory.INVERTER,
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
        self._client = Client(server_url=server_url, key=key)
        self._bg_service = DispatchScheduler(
            microgrid_id,
            self._client,
        )
        self._actor_dispatchers: dict[str, ActorDispatcher] = {}

    def start(self) -> None:
        """Start the local dispatch service."""
        self._bg_service.start()

    async def manage(
        self,
        dispatch_type: str,
        *,
        actor_factory: Callable[[DispatchInfo, Receiver[DispatchInfo]], Actor],
        merge_strategy: MergeByIdentity | None = None,
    ) -> None:
        """Manage actors for a given dispatch type.

        Creates and manages an ActorDispatcher for the given type that will
        start, stop and reconfigure actors based on received dispatches.

        Args:
            dispatch_type: The type of the dispatch to manage.
            actor_factory: The factory to create actors.
            merge_strategy: The strategy to merge running intervals.
        """
        dispatcher = self._actor_dispatchers.get(dispatch_type)

        if dispatcher is not None:
            _logger.debug(
                "Ignoring duplicate actor dispatcher request for %r", dispatch_type
            )
            return

        def id_identity(dispatch: Dispatch) -> int:
            return dispatch.id

        dispatcher = ActorDispatcher(
            actor_factory=actor_factory,
            running_status_receiver=await self.new_running_state_event_receiver(
                dispatch_type, merge_strategy=merge_strategy
            ),
            map_dispatch=(
                id_identity if merge_strategy is None else merge_strategy.identity
            ),
        )

        self._actor_dispatchers[dispatch_type] = dispatcher
        dispatcher.start()

    @property
    def client(self) -> Client:
        """Return the client."""
        return self._client

    def new_lifecycle_events_receiver(
        self, dispatch_type: str
    ) -> Receiver[DispatchEvent]:
        """Return new, updated or deleted dispatches receiver.

        Args:
            dispatch_type: The type of the dispatch to listen for.

        Returns:
            A new receiver for new dispatches.
        """
        return self._bg_service.new_lifecycle_events_receiver(dispatch_type)

    async def new_running_state_event_receiver(
        self,
        dispatch_type: str,
        *,
        merge_strategy: MergeStrategy | None = None,
    ) -> Receiver[Dispatch]:
        """Return running state event receiver.

        This receiver will receive a message whenever the current running
        status of a dispatch changes.

        Usually, one message per scheduled run is to be expected.
        However, things get complicated when a dispatch was modified:

        If it was currently running and the modification now says
        it should not be running or running with different parameters,
        then a message will be sent.

        In other words: Any change that is expected to make an actor start, stop
        or adjust itself according to new dispatch options causes a message to be
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

        `merge_strategy` is an instance of a class derived from
        [`MergeStrategy`][frequenz.dispatch.MergeStrategy] Available strategies
        are:

        * [`MergeByType`][frequenz.dispatch.MergeByType] — merges all dispatches
          of the same type
        * [`MergeByTypeTarget`][frequenz.dispatch.MergeByTypeTarget] — merges all
          dispatches of the same type and target
        * `None` — no merging, just send all events (default)

        Running intervals from multiple dispatches will be merged, according to
        the chosen strategy.

        While merging, stop events are ignored as long as at least one
        merge-criteria-matching dispatch remains active.

        Args:
            dispatch_type: The type of the dispatch to listen for.
            merge_strategy: The type of the strategy to merge running intervals.

        Returns:
            A new receiver for dispatches whose running status changed.
        """
        return await self._bg_service.new_running_state_event_receiver(
            dispatch_type, merge_strategy=merge_strategy
        )
