# License: MIT
# Copyright Â© 2024 Frequenz Energy-as-a-Service GmbH

"""Tests for the frequenz.dispatch.actor package."""

import asyncio
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from random import randint
from typing import AsyncIterator, Iterator
from unittest.mock import MagicMock

import async_solipsism
import time_machine
from frequenz.channels import Broadcast, Receiver
from frequenz.channels._broadcast import Sender
from frequenz.client.dispatch.test.client import FakeClient, to_create_params
from frequenz.client.dispatch.test.generator import DispatchGenerator
from frequenz.client.dispatch.types import Dispatch
from pytest import fixture

from frequenz.dispatch.actor import DispatchActor


# This method replaces the event loop for all tests in the file.
@fixture(scope="module")
def event_loop() -> Iterator[async_solipsism.EventLoop]:
    """Replace the loop with one that doesn't interact with the outside world."""
    loop = async_solipsism.EventLoop()
    yield loop
    loop.close()


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


@dataclass
class ActorTestEnv:
    """Test environment for the actor."""

    actor: DispatchActor
    """The actor under test."""
    updated_dispatches: Receiver[Dispatch]
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

    class YieldingSender(Sender[Dispatch]):
        """A sender that yields after sending.

        For testing we want to manipulate the time after a call to send.

        The normal sender normally doesn't yield/await, robbing us of the
        opportunity to manipulate the time.
        """

        def __init__(self, channel: Broadcast[Dispatch]) -> None:
            super().__init__(channel)

        async def send(self, msg: Dispatch) -> None:
            """Send the value and yield."""
            await super().send(msg)
            await asyncio.sleep(1)

    updated_dispatches = Broadcast[Dispatch]("updated_dispatches")
    ready_dispatches = Broadcast[Dispatch]("ready_dispatches")
    microgrid_id = randint(1, 100)

    actor = DispatchActor(
        microgrid_id=microgrid_id,
        grpc_channel=MagicMock(),
        svc_addr="localhost",
        updated_dispatch_sender=YieldingSender(updated_dispatches),
        ready_dispatch_sender=YieldingSender(ready_dispatches),
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
    await actor_env.client.create(**to_create_params(sample))

    dispatch = await actor_env.updated_dispatches.receive()
    sample.update_time = dispatch.update_time
    sample.create_time = dispatch.create_time
    sample.id = dispatch.id
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

    next_run = DispatchActor.calculate_next_run(dispatch, _now())
    assert next_run is not None

    fake_time.shift(next_run - _now() - timedelta(seconds=1))
    await asyncio.sleep(1)

    ready_dispatch = await actor_env.ready_dispatches.receive()

    assert ready_dispatch == dispatch
