# Dispatch Highlevel Interface Release Notes

## Summary

This is the first release of the highlevel dispatch interface!

## Upgrading

* `Dispatcher.ready_to_execute()` was renamed to `Dispatcher.running_status_change()`

## New Features

* Introduced new class `Dispatch` (based on the client class) that contains useful functions and extended information about the received dispatch.
* `Dispatcher.client` was added to provide an easy access to the client for updating, deleting and creating dispatches
