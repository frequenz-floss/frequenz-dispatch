# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* The method `Dispatch.running(type: str)` was replaced with the property `Dispatch.started: bool`.
* The SDK dependency was widened to allow versions up to (excluding) v1.0.0-rc1500

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

* Fixed a crash when reading a Dispatch with frequency YEARLY.
