# Dispatch Highlevel Interface

[![Build Status](https://github.com/frequenz-floss/frequenz-dispatch-python/actions/workflows/ci.yaml/badge.svg)](https://github.com/frequenz-floss/frequenz-dispatch-python/actions/workflows/ci.yaml)
[![PyPI Package](https://img.shields.io/pypi/v/frequenz-dispatch)](https://pypi.org/project/frequenz-dispatch/)
[![Docs](https://img.shields.io/badge/docs-latest-informational)](https://frequenz-floss.github.io/frequenz-dispatch-python/)

## Introduction

A highlevel interface for the dispatch API.
The interface is made of the dispatch actor which should run once per microgrid.
It provides two channels for clients:
- "new_dispatches" for newly created dispatches
- "ready_dispatches" for dispatches that are ready to be executed

## Example Usage

```python
async def run():
    # dispatch helper sends out dispatches when they are due
    dispatch_arrived = dispatch_helper.new_dispatches().new_receiver()
    dispatch_ready = dispatch_helper.ready_dispatches().new_receiver()

    async for selected in select(dispatch_ready, dispatch_arrived):
        if selected_from(selected, dispatch_arrived):
            dispatch = selected.value
            match dispatch.type:
                case DISPATCH_TYPE_BATTERY_CHARGE:
                    battery_pool = microgrid.battery_pool(dispatch.battery_set, task_id)
                    battery_pool.set_charge(dispatch.power)
            ...
        if selected_from(selected, dispatch_ready):
            dispatch = selected.value
            log.info("New dispatch arrived %s", dispatch)
```

## Supported Platforms

The following platforms are officially supported (tested):

- **Python:** 3.11
- **Operating System:** Ubuntu Linux 20.04
- **Architectures:** amd64, arm64

## Contributing

If you want to know how to build this project and contribute to it, please
check out the [Contributing Guide](CONTRIBUTING.md).
