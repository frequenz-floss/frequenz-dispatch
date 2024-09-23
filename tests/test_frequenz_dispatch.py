# License: MIT
# Copyright Â© 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.dispatch.actor package."""

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from random import randint
from typing import AsyncIterator, Iterator

import async_solipsism
import time_machine
from frequenz.channels import Broadcast, Receiver
from frequenz.client.dispatch.test.client import FakeClient, to_create_params
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import Dispatch as BaseDispatch
from frequenz.client.dispatch.types import Frequency, RecurrenceRule
from pytest import fixture

from frequenz.dispatch import (
    Created,
    Deleted,
    Dispatch,
    DispatchEvent,
    RunningState,
    Updated,
)
from frequenz.dispatch.actor import DispatchingActor


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


@dataclass(frozen=True)
class ActorTestEnv:
    """Test environment for the actor."""

    actor: DispatchingActor
    """The actor under test."""
    updated_dispatches: Receiver[DispatchEvent]
    """The receiver for updated dispatches."""
    running_state_change: Receiver[Dispatch]
    """The receiver for ready dispatches."""
    client: FakeClient
    """The fake client for the actor."""
    microgrid_id: int
    """The microgrid id."""


@fixture
async def actor_env() -> AsyncIterator[ActorTestEnv]:
    """Return an actor test environment."""
    lifecycle_updates_dispatches = Broadcast[DispatchEvent](name="lifecycle_updates")
    running_state_change_dispatches = Broadcast[Dispatch](name="running_state_change")
    microgrid_id = randint(1, 100)
    client = FakeClient()

    actor = DispatchingActor(
        microgrid_id=microgrid_id,
        lifecycle_updates_sender=lifecycle_updates_dispatches.new_sender(),
        running_state_change_sender=running_state_change_dispatches.new_sender(),
        client=client,
    )

    actor.start()
    try:
        yield ActorTestEnv(
            actor=actor,
            updated_dispatches=lifecycle_updates_dispatches.new_receiver(),
            running_state_change=running_state_change_dispatches.new_receiver(),
            client=client,
            microgrid_id=microgrid_id,
        )
    finally:
        await actor.stop()


@fixture
def generator() -> DispatchGenerator:
    """Return a dispatch generator."""
    return DispatchGenerator()


async def test_new_dispatch_created(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
) -> None:
    """Test that a new dispatch is created."""
    sample = generator.generate_dispatch()

    await _test_new_dispatch_created(actor_env, sample)


def update_dispatch(sample: BaseDispatch, dispatch: BaseDispatch) -> BaseDispatch:
    """Update the sample dispatch with the creation fields from the dispatch.

    Args:
        sample: The sample dispatch to update
        dispatch: The dispatch to update the sample with

    Returns:
        The updated sample dispatch
    """
    return replace(
        sample,
        update_time=dispatch.update_time,
        create_time=dispatch.create_time,
        id=dispatch.id,
    )


async def _test_new_dispatch_created(
    actor_env: ActorTestEnv,
    sample: BaseDispatch,
) -> Dispatch:
    """Test that a new dispatch is created.

    Args:
        actor_env: The actor environment
        sample: The sample dispatch to create

    Returns:
        The sample dispatch that was created
    """
    await actor_env.client.create(**to_create_params(actor_env.microgrid_id, sample))

    dispatch_event = await actor_env.updated_dispatches.receive()

    match dispatch_event:
        case Deleted(dispatch) | Updated(dispatch):
            assert False, "Expected a created event"
        case Created(dispatch):
            received = Dispatch(update_dispatch(sample, dispatch))
            received._set_running_status_notified()  # pylint: disable=protected-access
            dispatch._set_running_status_notified()  # pylint: disable=protected-access
            assert dispatch == received

    return dispatch


async def test_existing_dispatch_updated(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that an existing dispatch is updated."""
    sample = generator.generate_dispatch()
    sample = replace(
        sample,
        active=False,
        recurrence=replace(sample.recurrence, frequency=Frequency.DAILY),
    )

    fake_time.shift(timedelta(seconds=1))

    sample = await _test_new_dispatch_created(actor_env, sample)
    fake_time.shift(timedelta(seconds=1))

    updated = await actor_env.client.update(
        microgrid_id=actor_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={
            "active": True,
            "recurrence.frequency": Frequency.UNSPECIFIED,
        },
    )
    fake_time.shift(timedelta(seconds=1))

    dispatch_event = await actor_env.updated_dispatches.receive()
    match dispatch_event:
        case Created(dispatch) | Deleted(dispatch):
            assert False, f"Expected an updated event, got {dispatch_event}"
        case Updated(dispatch):
            assert dispatch == Dispatch(
                updated,
                running_state_change_synced=dispatch.running_state_change_synced,
            )


async def test_existing_dispatch_deleted(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that an existing dispatch is deleted."""
    sample = await _test_new_dispatch_created(actor_env, generator.generate_dispatch())

    await actor_env.client.delete(
        microgrid_id=actor_env.microgrid_id, dispatch_id=sample.id
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(10)

    dispatch_event = await actor_env.updated_dispatches.receive()
    match dispatch_event:
        case Created(dispatch) | Updated(dispatch):
            assert False, "Expected a deleted event"
        case Deleted(dispatch):
            sample._set_deleted()  # pylint: disable=protected-access
            dispatch._set_running_status_notified()  # pylint: disable=protected-access
            assert dispatch == sample


async def test_dispatch_inf_duration_deleted(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a dispatch with infinite duration can be deleted while running."""
    # Generate a dispatch with infinite duration (duration=None)
    sample = generator.generate_dispatch()
    sample = replace(
        sample, active=True, duration=None, start_time=_now() + timedelta(seconds=5)
    )
    # Create the dispatch
    sample = await _test_new_dispatch_created(actor_env, sample)
    # Advance time to when the dispatch should start
    fake_time.shift(timedelta(seconds=40))
    await asyncio.sleep(40)
    # Expect notification of the dispatch being ready to run
    ready_dispatch = await actor_env.running_state_change.receive()
    assert ready_dispatch.running(sample.type) == RunningState.RUNNING

    # Now delete the dispatch
    await actor_env.client.delete(
        microgrid_id=actor_env.microgrid_id, dispatch_id=sample.id
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification to stop the dispatch
    done_dispatch = await actor_env.running_state_change.receive()
    assert done_dispatch.running(sample.type) == RunningState.STOPPED


async def test_dispatch_inf_duration_updated_stopped_started(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a dispatch with infinite duration can be stopped and started by updating it."""
    # Generate a dispatch with infinite duration (duration=None)
    sample = generator.generate_dispatch()
    sample = replace(
        sample, active=True, duration=None, start_time=_now() + timedelta(seconds=5)
    )
    # Create the dispatch
    sample = await _test_new_dispatch_created(actor_env, sample)
    # Advance time to when the dispatch should start
    fake_time.shift(timedelta(seconds=40))
    await asyncio.sleep(40)
    # Expect notification of the dispatch being ready to run
    ready_dispatch = await actor_env.running_state_change.receive()
    assert ready_dispatch.running(sample.type) == RunningState.RUNNING

    # Now update the dispatch to set active=False (stop it)
    await actor_env.client.update(
        microgrid_id=actor_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={"active": False},
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification to stop the dispatch
    stopped_dispatch = await actor_env.running_state_change.receive()
    assert stopped_dispatch.running(sample.type) == RunningState.STOPPED

    # Now update the dispatch to set active=True (start it again)
    await actor_env.client.update(
        microgrid_id=actor_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={"active": True},
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification of the dispatch being ready to run again
    started_dispatch = await actor_env.running_state_change.receive()
    assert started_dispatch.running(sample.type) == RunningState.RUNNING


async def test_dispatch_inf_duration_updated_to_finite_and_stops(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test updating an inf. duration changing to finite.

    Test that updating an infinite duration dispatch to a finite duration causes
    it to stop if the duration has passed.
    """
    # Generate a dispatch with infinite duration (duration=None)
    sample = generator.generate_dispatch()
    sample = replace(
        sample, active=True, duration=None, start_time=_now() + timedelta(seconds=5)
    )
    # Create the dispatch
    sample = await _test_new_dispatch_created(actor_env, sample)
    # Advance time to when the dispatch should start
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification of the dispatch being ready to run
    ready_dispatch = await actor_env.running_state_change.receive()
    assert ready_dispatch.running(sample.type) == RunningState.RUNNING

    # Update the dispatch to set duration to a finite duration that has already passed
    # The dispatch has been running for 5 seconds; set duration to 5 seconds
    await actor_env.client.update(
        microgrid_id=actor_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={"duration": timedelta(seconds=5)},
    )
    # Advance time to allow the update to be processed
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)
    # Expect notification to stop the dispatch because the duration has passed
    stopped_dispatch = await actor_env.running_state_change.receive()
    assert stopped_dispatch.running(sample.type) == RunningState.STOPPED


async def test_dispatch_schedule(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a random dispatch is scheduled correctly."""
    sample = replace(
        generator.generate_dispatch(), active=True, duration=timedelta(seconds=10)
    )
    await actor_env.client.create(**to_create_params(actor_env.microgrid_id, sample))
    dispatch = Dispatch(actor_env.client.dispatches(actor_env.microgrid_id)[0])

    next_run = dispatch.next_run_after(_now())
    assert next_run is not None

    fake_time.shift(next_run - _now() - timedelta(seconds=1))
    await asyncio.sleep(1)

    # Expect notification of the dispatch being ready to run
    ready_dispatch = await actor_env.running_state_change.receive()

    # Set flag we expect to be different to compare the dispatch with the one received
    dispatch._set_running_status_notified()  # pylint: disable=protected-access

    assert ready_dispatch == dispatch

    assert dispatch.duration is not None
    # Shift time to the end of the dispatch
    fake_time.shift(dispatch.duration + timedelta(seconds=1))
    await asyncio.sleep(1)

    # Expect notification to stop the dispatch
    done_dispatch = await actor_env.running_state_change.receive()
    assert done_dispatch == dispatch


async def test_dispatch_inf_duration_updated_to_finite_and_continues(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that updating an infinite duration dispatch to a finite duration.

    Test that updating an infinite duration dispatch to a finite
    allows it to continue running if the duration hasn't passed.
    """
    # Generate a dispatch with infinite duration (duration=None)
    sample = generator.generate_dispatch()
    sample = replace(
        sample, active=True, duration=None, start_time=_now() + timedelta(seconds=5)
    )
    # Create the dispatch
    sample = await _test_new_dispatch_created(actor_env, sample)
    # Advance time to when the dispatch should start
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(1)
    # Expect notification of the dispatch being ready to run
    ready_dispatch = await actor_env.running_state_change.receive()
    assert ready_dispatch.running(sample.type) == RunningState.RUNNING

    # Update the dispatch to set duration to a finite duration that hasn't passed yet
    # The dispatch has been running for 5 seconds; set duration to 100 seconds
    await actor_env.client.update(
        microgrid_id=actor_env.microgrid_id,
        dispatch_id=sample.id,
        new_fields={"duration": timedelta(seconds=100)},
    )
    # Advance time slightly to process the update
    fake_time.shift(timedelta(seconds=1))
    await asyncio.sleep(1)
    # The dispatch should continue running
    # Advance time until the total running time reaches 100 seconds
    fake_time.shift(timedelta(seconds=94))
    await asyncio.sleep(1)
    # Expect notification to stop the dispatch because the duration has now passed
    stopped_dispatch = await actor_env.running_state_change.receive()
    assert stopped_dispatch.running(sample.type) == RunningState.STOPPED


async def test_dispatch_new_but_finished(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a dispatch that is already finished is not started."""
    # Generate a dispatch that is already finished
    finished_dispatch = generator.generate_dispatch()
    finished_dispatch = replace(
        finished_dispatch,
        active=True,
        duration=timedelta(seconds=5),
        start_time=_now() - timedelta(seconds=50),
        recurrence=RecurrenceRule(),
        type="I_SHOULD_NEVER_RUN",
    )
    # Create an old dispatch
    actor_env.client.set_dispatches(actor_env.microgrid_id, [finished_dispatch])
    await actor_env.actor.stop()
    actor_env.actor.start()

    # Create another dispatch the normal way
    new_dispatch = generator.generate_dispatch()
    new_dispatch = replace(
        new_dispatch,
        active=True,
        duration=timedelta(seconds=10),
        start_time=_now() + timedelta(seconds=5),
        recurrence=RecurrenceRule(),
        type="NEW_BETTER_DISPATCH",
    )
    # Consume one lifecycle_updates event
    await actor_env.updated_dispatches.receive()
    new_dispatch = await _test_new_dispatch_created(actor_env, new_dispatch)

    # Advance time to when the new dispatch should still not start
    fake_time.shift(timedelta(seconds=100))

    assert await actor_env.running_state_change.receive() == new_dispatch
