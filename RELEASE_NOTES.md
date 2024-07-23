# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

<!-- Here goes a list of breaking changes that users should be aware of when upgrading to this release -->

## Bug Fixes

* When a dispatch is deleted, the running state change notification telling the actor that it is no longer running will only be sent if there are no other dispatches of the same type running.
