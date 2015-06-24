Composite operations
====================

Tripod's composites (i.e. Views, Tables, and Search documents) can be expensive to invalidate and rebuild, especially with
large datasets or subjects that are heavily interconnected in other graphs.

To help offset this, Tripod lets you prioritize the regeneration of this data by allowing you to specify which operations
should be done synchronously when saving and which can be deferred to a background queue.

This is done when you initialize Tripod:

```php

$tripod = new \Tripod\Mongo\Driver('CBD_something', 'my_app', array(OP_ASYNC=>array(OP_VIEWS=>false, OP_TABLES=>true, OP_SEARCH=>true));

```

By default (if no OP_ASYNC array is passed), views will be regenerated synchronously and table rows and search documents
will be generated via a background job using php-resque.

Queues
------

Tripod divides composite regeneration into two jobs: DiscoverImpactedSubjects (which isolates the resources affected by
 the changes) and ApplyOperation (which actually invalidates the composite documents and regenerates the new ones).  These can
 be run either synchronously or asynchronously.

Queues are configured via environment variables, although defaults will be set if no environment variables are found.

` MONGO_TRIPOD_RESQUE_SERVER `
    defines the Redis backend for Resque (default: localhost:6379)

` APP_ENV `
    (Optional) - the queues will be namespaced to the particular environment (e.g. ` tripod:::production::discover `)

` TRIPOD_DISCOVER_QUEUE `
    The queue name for DiscoverImpactedSubjects jobs (if ` $APP_ENV ` is set, defaults to ` tripod::$APP_ENV::discover `, otherwise ` tripod::discover `)

` TRIPOD_APPLY_QUEUE `
    The queue name for ApplyOperations jobs (if ` $APP_ENV ` is set, defaults to ` tripod::$APP_ENV::apply `, otherwise ` tripod::apply `)

There are some generic php-resque job worker scripts in scripts/mongo to get you started.



