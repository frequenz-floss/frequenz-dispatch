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
from frequenz.client.dispatch.types import Frequency
from pytest import fixture

from frequenz.dispatch import Created, Deleted, Dispatch, DispatchEvent, Updated
from frequenz.dispatch.actor import DispatchingActor


# This method replaces the event loop for all tests in the file.
@fixture
def event_loop_policy() -> async_solipsism.EventLoopPolicy:
    """Return an event loop policy that uses the async solipsism event loop."""
    return async_solipsism.EventLoopPolicy()


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
    ready_dispatches: Receiver[Dispatch]
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

    yield ActorTestEnv(
        actor,
        lifecycle_updates_dispatches.new_receiver(),
        running_state_change_dispatches.new_receiver(),
        client,
        microgrid_id,
    )

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
) -> BaseDispatch:
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
            sample = update_dispatch(sample, dispatch)
            assert dispatch == Dispatch(sample)

    return sample


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

    await actor_env.client.update(
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
            assert False, "Expected an updated event"
        case Updated(dispatch):
            sample = update_dispatch(sample, dispatch)
            sample = replace(
                sample,
                active=True,
                recurrence=replace(sample.recurrence, frequency=Frequency.UNSPECIFIED),
            )

            assert dispatch == Dispatch(
                sample,
                running_state_change_synced=dispatch.running_state_change_synced,
            )


async def test_existing_dispatch_deleted(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that an existing dispatch is deleted."""
    sample = generator.generate_dispatch()

    sample = await _test_new_dispatch_created(actor_env, sample)

    await actor_env.client.delete(
        microgrid_id=actor_env.microgrid_id, dispatch_id=sample.id
    )
    fake_time.shift(timedelta(seconds=10))
    await asyncio.sleep(10)

    print("Awaiting deleted dispatch update")
    dispatch_event = await actor_env.updated_dispatches.receive()
    match dispatch_event:
        case Created(dispatch) | Updated(dispatch):
            assert False, "Expected a deleted event"
        case Deleted(dispatch):
            sample = update_dispatch(sample, dispatch)
            assert dispatch == Dispatch(sample, deleted=True)


async def test_dispatch_schedule(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a random dispatch is scheduled correctly."""
    sample = generator.generate_dispatch()
    await actor_env.client.create(**to_create_params(actor_env.microgrid_id, sample))
    dispatch = Dispatch(actor_env.client.dispatches(actor_env.microgrid_id)[0])

    next_run = dispatch.next_run_after(_now())
    assert next_run is not None

    fake_time.shift(next_run - _now() - timedelta(seconds=1))
    await asyncio.sleep(1)

    ready_dispatch = await actor_env.ready_dispatches.receive()

    assert ready_dispatch == dispatch
