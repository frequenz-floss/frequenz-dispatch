# Dispatch Highlevel Interface Release Notes

## Upgrading

* An API key for authorization must now be passed to the client.

## Bug Fixes

* When a dispatch is deleted, the running state change notification telling the actor that it is no longer running will only be sent if there are no other dispatches of the same type running.
