# LICENSE: ALL RIGHTS RESERVED
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Test the dispatch runner."""

import asyncio
import heapq
import logging
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Iterator, cast

import async_solipsism
import time_machine
from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.dispatch.recurrence import Frequency
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.sdk.actor import Actor
from pytest import fixture

from frequenz.dispatch import ActorDispatcher, Dispatch, DispatchUpdate
from frequenz.dispatch._bg_service import DispatchScheduler


@fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Set the event loop policy to use async_solipsism."""
    policy = async_solipsism.EventLoopPolicy()
    asyncio.set_event_loop_policy(policy)
    return policy


@fixture
def fake_time() -> Iterator[time_machine.Coordinates]:
    """Replace real time with a time machine that doesn't automatically tick."""
    # destination can be a datetime or a timestamp (int), so are moving to the
    # epoch (in UTC!)
    with time_machine.travel(destination=0, tick=False) as traveller:
        yield traveller


def _now() -> datetime:
    """Return the current time in UTC."""
    return datetime.now(tz=timezone.utc)


class MockActor(Actor):
    """Mock actor for testing."""

    def __init__(
        self, initial_dispatch: DispatchUpdate, receiver: Receiver[DispatchUpdate]
    ) -> None:
        """Initialize the actor."""
        super().__init__(name="MockActor")
        logging.info("MockActor created")
        self.initial_dispatch = initial_dispatch
        self.receiver = receiver

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(1)


@dataclass
class TestEnv:
    """Test environment."""

    actors_service: ActorDispatcher
    running_status_sender: Sender[Dispatch]
    generator: DispatchGenerator = DispatchGenerator()

    @property
    def actor(self) -> MockActor | None:
        """Return the actor."""
        # pylint: disable=protected-access
        if self.actors_service._actor is None:
            return None
        return cast(MockActor, self.actors_service._actor)
        # pylint: enable=protected-access

    @property
    def updates_receiver(self) -> Receiver[DispatchUpdate]:
        """Return the updates receiver."""
        assert self.actor is not None
        return self.actor.receiver


@fixture
async def test_env() -> AsyncIterator[TestEnv]:
    """Create a test environment."""
    channel = Broadcast[Dispatch](name="dispatch ready test channel")

    actors_service = ActorDispatcher(
        actor_factory=MockActor,
        running_status_receiver=channel.new_receiver(),
    )

    actors_service.start()
    await asyncio.sleep(1)

    yield TestEnv(
        actors_service=actors_service,
        running_status_sender=channel.new_sender(),
    )

    await actors_service.stop()


async def test_simple_start_stop(
    test_env: TestEnv,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test behavior when receiving start/stop messages."""
    now = _now()
    duration = timedelta(minutes=10)
    dispatch = test_env.generator.generate_dispatch()
    dispatch = replace(
        dispatch,
        active=True,
        dry_run=False,
        duration=duration,
        start_time=now,
        payload={"test": True},
        type="UNIT_TEST",
        recurrence=replace(
            dispatch.recurrence,
            frequency=Frequency.UNSPECIFIED,
        ),
    )

    # Send status update to start actor, expect no DispatchUpdate for the start
    await test_env.running_status_sender.send(Dispatch(dispatch))
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)
    await asyncio.sleep(1)
    logging.info("Sent dispatch")

    assert test_env.actor is not None
    event = test_env.actor.initial_dispatch
    assert event.options == {"test": True}
    assert event.components == dispatch.target
    assert event.dry_run is False

    logging.info("Received dispatch")

    assert test_env.actor is not None
    assert test_env.actor.is_running is True

    fake_time.shift(duration)
    await test_env.running_status_sender.send(Dispatch(dispatch))

    # Give await actor.stop a chance to run
    await asyncio.sleep(1)

    assert test_env.actor is None


def test_heapq_dispatch_compare(test_env: TestEnv) -> None:
    """Test that the heapq compare function works."""
    dispatch1 = test_env.generator.generate_dispatch()
    dispatch2 = test_env.generator.generate_dispatch()

    # Simulate two dispatches with the same 'until' time
    now = datetime.now(timezone.utc)
    until_time = now + timedelta(minutes=5)

    # Create the heap
    scheduled_events: list[DispatchScheduler.QueueItem] = []

    # Push two events with the same 'until' time onto the heap
    heapq.heappush(
        scheduled_events,
        DispatchScheduler.QueueItem(until_time, Dispatch(dispatch1), True),
    )
    heapq.heappush(
        scheduled_events,
        DispatchScheduler.QueueItem(until_time, Dispatch(dispatch2), True),
    )


def test_heapq_dispatch_start_stop_compare(test_env: TestEnv) -> None:
    """Test that the heapq compare function works."""
    dispatch1 = test_env.generator.generate_dispatch()
    dispatch2 = test_env.generator.generate_dispatch()

    # Simulate two dispatches with the same 'until' time
    now = datetime.now(timezone.utc)
    until_time = now + timedelta(minutes=5)

    # Create the heap
    scheduled_events: list[DispatchScheduler.QueueItem] = []

    # Push two events with the same 'until' time onto the heap
    heapq.heappush(
        scheduled_events,
        DispatchScheduler.QueueItem(until_time, Dispatch(dispatch1), stop_event=False),
    )
    heapq.heappush(
        scheduled_events,
        DispatchScheduler.QueueItem(until_time, Dispatch(dispatch2), stop_event=True),
    )

    assert scheduled_events[0].dispatch_id == dispatch1.id
    assert scheduled_events[1].dispatch_id == dispatch2.id


async def test_dry_run(test_env: TestEnv, fake_time: time_machine.Coordinates) -> None:
    """Test the dry run mode."""
    dispatch = test_env.generator.generate_dispatch()
    dispatch = replace(
        dispatch,
        dry_run=True,
        active=True,
        start_time=_now(),
        duration=timedelta(minutes=10),
        type="UNIT_TEST",
        recurrence=replace(
            dispatch.recurrence,
            frequency=Frequency.UNSPECIFIED,
        ),
    )

    await test_env.running_status_sender.send(Dispatch(dispatch))
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)

    assert test_env.actor is not None
    event = test_env.actor.initial_dispatch

    assert event.dry_run is dispatch.dry_run
    assert event.components == dispatch.target
    assert event.options == dispatch.payload
    assert test_env.actor is not None
    assert test_env.actor.is_running is True

    assert dispatch.duration is not None
    fake_time.shift(dispatch.duration)
    await test_env.running_status_sender.send(Dispatch(dispatch))

    # Give await actor.stop a chance to run
    await asyncio.sleep(1)

    assert test_env.actor is None
