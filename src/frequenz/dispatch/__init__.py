# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A highlevel interface for the dispatch API.

A small overview of the most important classes in this module:

* [Dispatcher][frequenz.dispatch.Dispatcher]: The entry point for the API.
* [Dispatch][frequenz.dispatch.Dispatch]: A dispatch type with lots of useful extra functionality.
* [DispatchManagingActor][frequenz.dispatch.DispatchManagingActor]: An actor to
    manage other actors based on incoming dispatches.
* [Created][frequenz.dispatch.Created],
  [Updated][frequenz.dispatch.Updated],
  [Deleted][frequenz.dispatch.Deleted]: Dispatch event types.

"""

from ._dispatch import Dispatch, RunningState
from ._dispatcher import Dispatcher, ReceiverFetcher
from ._event import Created, Deleted, DispatchEvent, Updated
from ._managing_actor import DispatchManagingActor, DispatchUpdate

__all__ = [
    "Created",
    "Deleted",
    "DispatchEvent",
    "Dispatcher",
    "ReceiverFetcher",
    "Updated",
    "Dispatch",
    "RunningState",
    "DispatchManagingActor",
    "DispatchUpdate",
]
