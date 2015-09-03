Event Hooks
===

Tripod has the concept of Event Hooks for you to be able to hook in custom code which will be executed when certain events are triggered.

Your custom code should be packacked in the form of a class that extends `\Tripod\IEventHook`. This means it will implement three methods:

* `pre` - will be executed just before the event occurs
* `success` - will be executed if the event is successful
* `failure` - will be executed if the event was attempted, but deemed to fail

These methods are each passed an `$args` array, the contents of which depend on the event type

Hooks cannot influence the execution flow by throwing exceptions - any exceptions are logged but not propagated.

Register a hook by calling `IDriver::registerHook` passing the event type constant as the first arg and an instance of your class as the second

Type: Save Changes 
---

`\Tripod\IEventHook::EVENT_SAVE_CHANGES`

This event is triggered when `\Tripod\IDriver::saveChanges()` is called.

Tripod will call `pre()`/`failure()/success()` with the following arg key values:

1. `pod` The pod that is being saved to (`string`)
1. `oldGraph` The old graph state being presented for save (`\Tripod\ExtendedGraph`)
1. `newGraph` The new graph state being presented for save (`\Tripod\ExtendedGraph`)
1. `context` The named graph being saved into  (`string`)

There are some additional values added for `success()`:

1. `changeSet` The complete changeset applied (`\ITripod\ChangeSet`)
1. `subjectsAndPredicatesOfChange` Subjects and predicates actually updated, keyed by subject (`array`)
1. `transaction_id` The resulting transaction ID in the tlog (`string`)



