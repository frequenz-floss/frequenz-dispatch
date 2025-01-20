# Dispatch Highlevel Interface Release Notes

## Summary

<!-- Here goes a general summary of what this release is about -->

## Upgrading

* Two properties have been replaced by methods that require a type as parameter.
    * `Dispatcher.lifecycle_events` has been replaced by the method `Dispatcher.new_lifecycle_events_receiver(self, dispatch_type: str)`.
    * `Dispatcher.running_status_change` has been replaced by the method `Dispatcher.new_running_state_event_receiver(self, dispatch_type: str, unify_running_intervals: bool)`.
* The managing actor constructor no longer requires the `dispatch_type` parameter. Instead you're expected to pass the type to the new-receiver function.

## New Features

* A new feature "unify running intervals" has been added to the `Dispatcher.new_running_state_event_receiver` method. Using it, you can automatically merge & unify consecutive and overlapping dispatch start/stop events of the same type. E.g. dispatch `A` starting at 10:10 and ending at 10:30 and dispatch `B` starts at 10:30 until 11:00, with the feature enabled this would in total trigger one start event, one reconfigure event at 10:30 and one stop event at 11:00.

## Bug Fixes

<!-- Here goes notable bug fixes that are worth a special mention or explanation -->
