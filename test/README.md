Notes for running test suite
============================

To test on multiple clusters, be sure the mongo connection information is saved as the environment variables:

TRIPOD_DATASOURCE_RS1_CONFIG

TRIPOD_DATASOURCE_RS2_CONFIG

They should be a json string that takes the form:

```
{
            "type" : "mongo",
            "connection": "mongodb:\/\/localhost",
            "replicaSet": ""
}
```

