# LICENSE: ALL RIGHTS RESERVED
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""Test the dispatch runner."""

import asyncio
import heapq
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from typing import AsyncIterator, Iterator

import async_solipsism
import time_machine
from frequenz.channels import Broadcast, Receiver, Sender
from frequenz.client.dispatch.recurrence import Frequency
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.sdk.actor import Actor
from pytest import fixture

from frequenz.dispatch import Dispatch, DispatchManagingActor, DispatchUpdate
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

    async def _run(self) -> None:
        while True:
            await asyncio.sleep(1)


@dataclass
class TestEnv:
    """Test environment."""

    actor: Actor
    runner_actor: DispatchManagingActor
    running_status_sender: Sender[Dispatch]
    updates_receiver: Receiver[DispatchUpdate]
    generator: DispatchGenerator = DispatchGenerator()


@fixture
async def test_env() -> AsyncIterator[TestEnv]:
    """Create a test environment."""
    channel = Broadcast[Dispatch](name="dispatch ready test channel")
    updates_channel = Broadcast[DispatchUpdate](name="dispatch update test channel")

    actor = MockActor()

    runner_actor = DispatchManagingActor(
        actor=actor,
        running_status_receiver=channel.new_receiver(),
        updates_sender=updates_channel.new_sender(),
    )

    # pylint: disable=protected-access
    runner_actor._restart_limit = 0
    runner_actor.start()

    yield TestEnv(
        actor=actor,
        runner_actor=runner_actor,
        running_status_sender=channel.new_sender(),
        updates_receiver=updates_channel.new_receiver(),
    )

    await runner_actor.stop()


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

    await test_env.running_status_sender.send(Dispatch(dispatch))
    fake_time.shift(timedelta(seconds=1))

    event = await test_env.updates_receiver.receive()
    assert event.options == {"test": True}
    assert event.components == dispatch.target
    assert event.dry_run is False

    assert test_env.actor.is_running is True

    fake_time.shift(duration)
    await test_env.running_status_sender.send(Dispatch(dispatch))

    # Give await actor.stop a chance to run in DispatchManagingActor
    await asyncio.sleep(0.1)

    assert test_env.actor.is_running is False


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

    event = await test_env.updates_receiver.receive()

    assert event.dry_run is dispatch.dry_run
    assert event.components == dispatch.target
    assert event.options == dispatch.payload
    assert test_env.actor.is_running is True

    assert dispatch.duration is not None
    fake_time.shift(dispatch.duration)
    await test_env.running_status_sender.send(Dispatch(dispatch))

    # Give await actor.stop a chance to run in DispatchManagingActor
    await asyncio.sleep(0.1)

    assert test_env.actor.is_running is False
