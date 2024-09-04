# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

- The dispatch high level interface now depends on `frequenz-sdk` version `v1.0.0-rc900`.
- We are now using the version `0.6.0` of the underlying `frequenz-client-dispatch` client library.
- The init parameter of the `Dispatcher` class has been changed to accept a `server_url` instead.

## New Features

* Using the new dispatch client, we now have support for pagination in the dispatch list request.
* The new client version also supports streaming, however it is not yet used internally in the high level interface.

## Bug Fixes

- Fix documentation cross-linking to the `frequenz-client-dispatch` package.
