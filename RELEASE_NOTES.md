# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Two properties have been replaced by methods that require a type as parameter.
    * `Dispatcher.lifecycle_events` has been replaced by the method `Dispatcher.new_lifecycle_events_receiver(self, type: str)`.
    * `Dispatcher.running_status_change` has been replaced by the method `Dispatcher.new_running_state_event_receiver(self, type: str)`.

## New Features

<!-- Here goes the main new features and examples or instructions on how to use them -->

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
