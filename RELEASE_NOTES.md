# Dispatch Highlevel Interface Release Notes

## Summary

This release introduces a more flexible and powerful mechanism for managing dispatch events with new strategies for merging intervals, enhanced customization options, and better overall alignment with evolving SDK dependencies. It also simplifies actor initialization while maintaining robust support for diverse dispatch scenarios.

## Upgrading

* `Dispatcher.start` is no longer `async`. Remove `await` when calling it.
* Two properties have been replaced by methods that require a type as parameter.
    * `Dispatcher.lifecycle_events` has been replaced by the method `Dispatcher.new_lifecycle_events_receiver(self, dispatch_type: str)`.
    * `Dispatcher.running_status_change` has been replaced by the method `Dispatcher.new_running_state_event_receiver(self, dispatch_type: str, unify_running_intervals: bool)`.
* The managing actor constructor no longer requires the `dispatch_type` parameter. Instead you're expected to pass the type to the new-receiver function.
* The `DispatchManagingActor` class has been renamed to `DispatchActorsService`.
    * It's interface has been simplified and now only requires an actor factory and a running status receiver.
    * It only supports a single actor at a time now.
    * Refer to the updated [usage example](https://frequenz-floss.github.io/frequenz-dispatch-python/latest/reference/frequenz/dispatch/#frequenz.dispatch.DispatchActorsService) for more information.
* `DispatchUpdate` was renamed to `DispatchInfo`.

## New Features

* A new feature "merger strategy" (`MergeByType`, `MergeByTypeTarget`) has been added to the `Dispatcher.new_running_state_event_receiver` method. Using it, you can automatically merge & unify consecutive and overlapping dispatch start/stop events of the same type. E.g. dispatch `A` starting at 10:10 and ending at 10:30 and dispatch `B` starts at 10:30 until 11:00, with the feature enabled this would in total trigger one start event, one reconfigure event at 10:30 and one stop event at 11:00.

* The SDK dependency was widened to allow versions up to (excluding) v1.0.0-rc1600.

