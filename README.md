# Dispatch Highlevel Interface

[![Build Status](https://github.com/frequenz-floss/frequenz-dispatch-python/actions/workflows/ci.yaml/badge.svg)](https://github.com/frequenz-floss/frequenz-dispatch-python/actions/workflows/ci.yaml)
[![PyPI Package](https://img.shields.io/pypi/v/frequenz-dispatch)](https://pypi.org/project/frequenz-dispatch/)
[![Docs](https://img.shields.io/badge/docs-latest-informational)](https://frequenz-floss.github.io/frequenz-dispatch-python/)

## Introduction

A highlevel interface for the dispatch API.

See [the documentation](https://frequenz-floss.github.io/frequenz-dispatch-python/v0.1/reference/frequenz/dispatch) for more information.

## Usage

The [`Dispatcher` class](https://frequenz-floss.github.io/frequenz-dispatch-python/v0.1/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher), the main entry point for the API, provides two channels:

* [Lifecycle events](https://frequenz-floss.github.io/frequenz-dispatch-python/v0.1/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher.lifecycle_events): A channel that sends a message whenever a [Dispatch][frequenz.dispatch.Dispatch] is created, updated or deleted.
* [Running status change](https://frequenz-floss.github.io/frequenz-dispatch-python/v0.1/reference/frequenz/dispatch/#frequenz.dispatch.Dispatcher.running_status_change): Sends a dispatch message whenever a dispatch is ready to be executed according to the schedule or the running status of the dispatch changed in a way that could potentially require the actor to start, stop or reconfigure itself.

### Example using the running status change channel

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

    changed_running_status_rx = dispatcher.running_status_change.new_receiver()

    async for dispatch in changed_running_status_rx:
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

## Supported Platforms

The following platforms are officially supported (tested):

- **Python:** 3.11
- **Operating System:** Ubuntu Linux 20.04
- **Architectures:** amd64, arm64

## Contributing

If you want to know how to build this project and contribute to it, please
check out the [Contributing Guide](CONTRIBUTING.md).
