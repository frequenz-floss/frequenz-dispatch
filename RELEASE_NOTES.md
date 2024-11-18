# Dispatch Highlevel Interface Release Notes

## Summary

* Updates lots of dependencies and through those gets a few new features:
 * `start_immediately` when creating dispatches is now supported.
 * `http2 keepalive` is now supported and enabled by default.
 * Some bugfixes from the channels & sdk libraries. are now included.


## Upgrading

<!-- Here goes notes on how to upgrade from previous versions, including deprecations and what they should be replaced with -->

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

* Fixed a crash in the `DispatchManagingActor` when dispatches shared an equal start time.
