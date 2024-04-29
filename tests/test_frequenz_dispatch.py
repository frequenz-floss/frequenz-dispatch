# License: MIT
# Copyright Â© 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.dispatch.actor package."""

import asyncio
from dataclasses import dataclass, replace
from datetime import datetime, timedelta, timezone
from random import randint
from typing import AsyncIterator, Iterator
from unittest.mock import MagicMock

import async_solipsism
import time_machine
from frequenz.channels import Broadcast, Receiver
from frequenz.client.dispatch.test.client import FakeClient, to_create_params
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import Dispatch, Frequency
from pytest import fixture

from frequenz.dispatch import Created, Deleted, DispatchEvent, Updated
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
    updated_dispatches = Broadcast[DispatchEvent](name="updated_dispatches")
    ready_dispatches = Broadcast[Dispatch](name="ready_dispatches")
    microgrid_id = randint(1, 100)

    actor = DispatchingActor(
        microgrid_id=microgrid_id,
        grpc_channel=MagicMock(),
        svc_addr="localhost",
        updated_dispatch_sender=updated_dispatches.new_sender(),
        ready_dispatch_sender=ready_dispatches.new_sender(),
    )

    client = FakeClient()
    actor._client = client  # pylint: disable=protected-access

    actor.start()

    yield ActorTestEnv(
        actor,
        updated_dispatches.new_receiver(),
        ready_dispatches.new_receiver(),
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
    sample = generator.generate_dispatch(actor_env.microgrid_id)

    await _test_new_dispatch_created(actor_env, sample)


async def _test_new_dispatch_created(
    actor_env: ActorTestEnv,
    sample: Dispatch,
) -> Dispatch:
    """Test that a new dispatch is created.

    Args:
        actor_env: The actor environment
        sample: The sample dispatch to create

    Returns:
        The sample dispatch that was created
    """
    await actor_env.client.create(**to_create_params(sample))

    dispatch_event = await actor_env.updated_dispatches.receive()

    match dispatch_event:
        case Deleted(dispatch) | Updated(dispatch):
            assert False, "Expected a created event"
        case Created(dispatch):
            sample = replace(
                sample,
                update_time=dispatch.update_time,
                create_time=dispatch.create_time,
                id=dispatch.id,
            )
            assert dispatch == sample

    return sample


async def test_existing_dispatch_updated(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that an existing dispatch is updated."""
    sample = generator.generate_dispatch(actor_env.microgrid_id)
    sample = replace(
        sample,
        active=False,
        recurrence=replace(sample.recurrence, frequency=Frequency.DAILY),
    )

    fake_time.shift(timedelta(seconds=1))

    sample = await _test_new_dispatch_created(actor_env, sample)
    fake_time.shift(timedelta(seconds=1))

    await actor_env.client.update(
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
            sample = replace(
                sample,
                active=True,
                recurrence=replace(sample.recurrence, frequency=Frequency.UNSPECIFIED),
                update_time=dispatch.update_time,
            )

            assert dispatch == sample


async def test_existing_dispatch_deleted(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that an existing dispatch is deleted."""
    sample = generator.generate_dispatch(actor_env.microgrid_id)

    sample = await _test_new_dispatch_created(actor_env, sample)

    await actor_env.client.delete(sample.id)
    fake_time.shift(timedelta(seconds=1))

    dispatch_event = await actor_env.updated_dispatches.receive()
    match dispatch_event:
        case Created(dispatch) | Updated(dispatch):
            assert False, "Expected a deleted event"
        case Deleted(dispatch):
            sample = replace(sample, update_time=dispatch.update_time)
            assert dispatch == sample


async def test_dispatch_schedule(
    actor_env: ActorTestEnv,
    generator: DispatchGenerator,
    fake_time: time_machine.Coordinates,
) -> None:
    """Test that a random dispatch is scheduled correctly."""
    sample = generator.generate_dispatch(actor_env.microgrid_id)
    await actor_env.client.create(**to_create_params(sample))
    dispatch = actor_env.client.dispatches[0]

    next_run = DispatchingActor.calculate_next_run(dispatch, _now())
    assert next_run is not None

    fake_time.shift(next_run - _now() - timedelta(seconds=1))
    await asyncio.sleep(1)

    ready_dispatch = await actor_env.ready_dispatches.receive()

    assert ready_dispatch == dispatch
