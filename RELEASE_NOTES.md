# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* `Dispatcher.running_state_change` now also sends a message when the duration specified in the dispatch has passed. If no duration is specified, no STOPPED message will be sent.

## New Features

* We now provide the `DispatchManagingActor` class, a class to manage actors based on incoming dispatches.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
