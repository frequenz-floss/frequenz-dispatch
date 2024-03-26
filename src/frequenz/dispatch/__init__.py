# License: MIT
# Copyright © 2024 Frequenz Energy-as-a-Service GmbH

"""A highlevel interface for the dispatch API."""

from frequenz.dispatch._dispatcher import Dispatcher
from frequenz.dispatch.actor import DispatchActor, DispatchEvent

__all__ = ["Dispatcher"]
