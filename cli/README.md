# SpecMesh CLI

Commands to provision, export and capture production & consumption chargeback metrics for a SpecMesh app (aka data product - AsyncApi.yml)

This page also contains a simple docker guide for local testing.

[See further down the page for setting up a Docker environment](https://github.com/specmesh/specmesh-build/tree/main/cli#quickstart-using-docker-on-the-local-machine) 


## Command: Provision

This command will provision Kafka resources using AsyncApi spec (aka. App, or data product) and publish to the configured cluster and schema registry environment. It can be run manually, and also as part of a GitOps workflow, and/or build promotion of the spec into different environments where cluster and SR endpoints are configured as environment variables.

### Common  config
`provision` will look for a `provision.properties` file in the docker /app/ folder (i.e. /app/provision.properties)
A default config file can optionally be used for managing/accessing common properties.


File provision.properties (automatically loaded from docker `/app/provision.properties`)
```properties
spec=/app/simple_schema_demo-api.yaml
acl.enabled=true
sr.enabled=true
dry.run=true
bootstrap.server=broker1:9092
username=admin
secret=nothing
schema.registry=http://schema-registry:8081
schema.path=/app/1
sr.api.key=admin
sr.api.secret=nothing

```

### Usage

> % docker run --rm --network kafka_network -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli  provision -bs kafka:9092  -sr http://schema-registry:8081 -spec /app/simple_schema_demo-api.yaml -schemaPath /app
> 

<details>
  <summary>Long form</summary>

```
 Usage: provision [-aclEnabled] [-clean] [-dry] [-srEnabled] [-bs=<brokerUrl>]
                 [-s=<secret>] [-schemaPath=<schemaPath>] [-spec=<spec>]
                 [-sr=<schemaRegistryUrl>] [-srKey=<srApiKey>]
                 [-srSecret=<srApiSecret>] [-u=<username>]
                 [-D=<String=String>]...
Apply a specification.yaml to provision kafka resources on a cluster.
Use 'provision.properties' for common arguments
 Explicit properties file location /app/provision.properties


      -aclEnabled, --acl-enabled
                             True (default) will provision/publish/validate
                               ACls. False will ignore ACL related operations
      -bs, --bootstrap-server=<brokerUrl>
                             Kafka bootstrap server url
      -clean, --clean-unspecified
                             Compares the cluster resources against the spec,
                               outputting proposed set of resources that are
                               unexpected (not specified). Use with '-dry-run'
                               for non-destructive checks. This operation will
                               not create resources, it will only remove
                               unspecified resources
      -D, --property=<String=String>
                             Specify Java runtime properties for Apache Kafka.
      -dry, --dry-run        Compares the cluster resources against the spec,
                               outputting proposed changes if  compatible. If
                               the spec incompatible with the cluster then will
                               fail with a descriptive error message. A return
                               value of '0' = indicates no  changes needed; '1'
                               = changes needed; '-1' not compatible
      -s, --secret=<secret>      secret credential for the cluster connection
      -schemaPath, --schema-path=<schemaPath>
                             schemaPath where the set of referenced schemas
                               will be loaded
      -spec, --spec=<spec>   specmesh specification file
      -sr, --schema-registry=<schemaRegistryUrl>
                             schemaRegistryUrl
      -srEnabled, --sr-enabled
                             True (default) will provision/publish/validate
                               schemas. False will ignore schema related
                               operations
      -srKey, --sr-api-key=<srApiKey>
                             srApiKey for schema registry
      -srSecret, --sr-api-secret=<srApiSecret>
                             srApiSecret for schema secret
      -u, --username=<username>  username or api key for the cluster connection
  
```
</details>
 

This demonstrates `provision`ing a *spec* into a docker environment, with a network `kafka_network`, and `kafka` is the container running a Kafka broker container, and `schema-registry` the schema registry container. 
 
### Output

```yaml
{
  "topics" : [ {
    "name" : "simple.spec_demo._public.user_signed_up",
    "state" : "CREATED",
    "partitions" : 3,
    "replication" : 1,
    "config" : {
      "cleanup.policy" : "delete"
    },
    "exception" : null,
    "messages" : ""
  }, {
    "schemas" : null,
    "acls" : [ {
      "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo._public, patternType=PREFIXED), entry=(principal=User:*, host=*, operation=READ, permissionType=ALLOW))",
      "state" : "CREATED",
      "aclBinding" : {
        "pattern" : {
          "resourceType" : "TOPIC",
          "name" : "simple.spec_demo._public",
          "patternType" : "PREFIXED",
          "unknown" : false
        },
        "entry" : {
          "data" : {
            "principal" : "User:*",
            "host" : "*",
            "operation" : "READ",
            "permissionType" : "ALLOW",
            "clusterLinkIds" : [ ]
          }        
 <<SNIP>>
```

<details>
  <summary>Long form</summary>

```yaml
{
  "topics" : [ {
    "name" : "simple.spec_demo._public.user_signed_up",
    "state" : "CREATED",
    "partitions" : 3,
    "replication" : 1,
    "config" : {
      "cleanup.policy" : "delete"
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "simple.spec_demo._private.user_checkout",
    "state" : "CREATED",
    "partitions" : 3,
    "replication" : 1,
    "config" : {
      "cleanup.policy" : "delete"
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "simple.spec_demo._protected.purchased",
    "state" : "CREATED",
    "partitions" : 3,
    "replication" : 1,
    "config" : { },
    "exception" : null,
    "messages" : ""
  } ],
  "schemas" : null,
  "acls" : [ {
    "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo._protected.purchased, patternType=LITERAL), entry=(principal=User:some.other.domain.root, host=*, operation=READ, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TOPIC",
        "name" : "simple.spec_demo._protected.purchased",
        "patternType" : "LITERAL",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:some.other.domain.root",
          "host" : "*",
          "operation" : "READ",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo._private, patternType=PREFIXED), entry=(principal=User:simple.spec_demo, host=*, operation=CREATE, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TOPIC",
        "name" : "simple.spec_demo._private",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:simple.spec_demo",
          "host" : "*",
          "operation" : "CREATE",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=GROUP, name=simple.spec_demo, patternType=PREFIXED), entry=(principal=User:simple.spec_demo, host=*, operation=READ, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "GROUP",
        "name" : "simple.spec_demo",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:simple.spec_demo",
          "host" : "*",
          "operation" : "READ",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo._public, patternType=PREFIXED), entry=(principal=User:*, host=*, operation=READ, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TOPIC",
        "name" : "simple.spec_demo._public",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:*",
          "host" : "*",
          "operation" : "READ",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TRANSACTIONAL_ID, name=simple.spec_demo, patternType=PREFIXED), entry=(principal=User:simple.spec_demo, host=*, operation=DESCRIBE, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TRANSACTIONAL_ID",
        "name" : "simple.spec_demo",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:simple.spec_demo",
          "host" : "*",
          "operation" : "DESCRIBE",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo, patternType=PREFIXED), entry=(principal=User:simple.spec_demo, host=*, operation=WRITE, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TOPIC",
        "name" : "simple.spec_demo",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:simple.spec_demo",
          "host" : "*",
          "operation" : "WRITE",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo._protected.purchased, patternType=LITERAL), entry=(principal=User:some.other.domain.root, host=*, operation=DESCRIBE, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TOPIC",
        "name" : "simple.spec_demo._protected.purchased",
        "patternType" : "LITERAL",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:some.other.domain.root",
          "host" : "*",
          "operation" : "DESCRIBE",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo, patternType=PREFIXED), entry=(principal=User:simple.spec_demo, host=*, operation=READ, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TOPIC",
        "name" : "simple.spec_demo",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:simple.spec_demo",
          "host" : "*",
          "operation" : "READ",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TRANSACTIONAL_ID, name=simple.spec_demo, patternType=PREFIXED), entry=(principal=User:simple.spec_demo, host=*, operation=WRITE, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TRANSACTIONAL_ID",
        "name" : "simple.spec_demo",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:simple.spec_demo",
          "host" : "*",
          "operation" : "WRITE",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=CLUSTER, name=kafka-cluster, patternType=PREFIXED), entry=(principal=User:simple.spec_demo, host=*, operation=IDEMPOTENT_WRITE, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "CLUSTER",
        "name" : "kafka-cluster",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:simple.spec_demo",
          "host" : "*",
          "operation" : "IDEMPOTENT_WRITE",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo, patternType=PREFIXED), entry=(principal=User:simple.spec_demo, host=*, operation=DESCRIBE, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TOPIC",
        "name" : "simple.spec_demo",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:simple.spec_demo",
          "host" : "*",
          "operation" : "DESCRIBE",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  }, {
    "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo._public, patternType=PREFIXED), entry=(principal=User:*, host=*, operation=DESCRIBE, permissionType=ALLOW))",
    "state" : "CREATED",
    "aclBinding" : {
      "pattern" : {
        "resourceType" : "TOPIC",
        "name" : "simple.spec_demo._public",
        "patternType" : "PREFIXED",
        "unknown" : false
      },
      "entry" : {
        "data" : {
          "principal" : "User:*",
          "host" : "*",
          "operation" : "DESCRIBE",
          "permissionType" : "ALLOW",
          "clusterLinkIds" : [ ]
        },
        "unknown" : false
      },
      "unknown" : false
    },
    "exception" : null,
    "messages" : ""
  } ]
}
  
```
</details>

## Command: Storage (chargeback metrics)

Reports app-id => topic-partition level metrics for storage bytes and offsets

> docker run --rm --network kafka_network -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli storage -bs kafka:9092 -spec /app/simple_spec_demo-api.yaml


<details>
  <summary>Long form</summary>

```
Usage: storage [-bs=<brokerUrl>] [-s=<secret>] [-spec=<spec>]
                     [-u=<username>]
Given a spec, break down the storage volume (including replication) against
each of its topic in bytes
      -bs, --bootstrap-server=<brokerUrl>
                             Kafka bootstrap server url
  -s, --secret=<secret>      secret credential for the cluster connection
      -spec, --spec=<spec>   specmesh specification file
  -u, --username=<username>  username or api key for the cluster connection
```
</details>


### Output

Topic data that matches the app-id (prefixed with `simple.spec_demo` within the api.yaml)

```json
 {"simple.spec_demo._private.user_checkout":{"storage":1590,"offset-total":6},"simple.spec_demo._protected.purchased":{"storage":0,"offset-total":0},"simple.spec_demo._public.user_signed_up":{"storage":9185,"offset-total":57}}
```

## Command: Consumption (chargeback metrics)

Report active consumer groups (with offset) that is consuming data given an app-ids set of topics.

>%  docker run --rm --network kafka_network -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli consumption -bs kafka:9092 -spec /app/simple_spec_demo-api.yaml


<details>
  <summary>Long form</summary>

```
Usage: consumption [-bs=<brokerUrl>] [-s=<secret>] [-spec=<spec>]
                   [-u=<username>]
Given a spec, break down the consumption volume against each of its topic
      -bs, --bootstrap-server=<brokerUrl>
                             Kafka bootstrap server url
  -s, --secret=<secret>      secret credential for the cluster connection
      -spec, --spec=<spec>   specmesh specification file
  -u, --username=<username>  username or api key for the cluster connection

``` 
</details>

### Output

A consumer group `some.other.app` with id `console-consumer...` is actively consuming data

```json
{"simple.spec_demo._public.user_signed_up":{"id":"some.other.app","members":[{"id":"console-consumer-7f9d23c7-a627-41cd-ade9-3919164bc363","clientId":"console-consumer","host":"/172.30.0.3","partitions":[{"id":0,"topic":"simple.spec_demo._public.user_signed_up","offset":57,"timestamp":-1}]}],"offsetTotal":57}}
```

 


## Command: Export to a spec

>  docker run --rm --network kafka_network -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli export -bs kafka:9092 -aggid simple:spec_demo

<details>
  <summary>Long form</summary>

```
Usage: export [-aggid=<aggid>] [-bs=<brokerUrl>] [-s=<secret>] [-u=<username>]
Build an incomplete spec from a running Cluster
      -aggid, --agg-id=<aggid>
                          specmesh - agg-id/prefix - aggregate identified
                            (app-id) to export against
      -bs, --bootstrap-server=<brokerUrl>
                          Kafka bootstrap server url
  -s, --secret=<secret>   secret credential for the cluster connection
  -u, --username=<username>
                          username or api key for the cluster connection
```
</details>

### Output

```json
{
  "id": "urn:simple.spec_demo",
  "version": "2023-05-06",
  "asyncapi": "2.5.0",
  "channels": {
    "_public.user_signed_up": {
      "bindings": {
        "kafka": {
          "partitions": 1,
          "replicas": 1,
          "configs": {
            "compression.type": "producer",
            "leader.replication.throttled.replicas": "",
            "message.downconversion.enable": "true",
            "min.insync.replicas": "1",
            "segment.jitter.ms": "0",
            "cleanup.policy": "delete",
            "flush.ms": "9223372036854775807",
            "follower.replication.throttled.replicas": "",
            "segment.bytes": "1073741824",
            "retention.ms": "604800000",
   <<SNIP>> 
```
Full JSON
```json
{"id":"urn:simple:spec_demo","version":"2023-05-06","asyncapi":"2.5.0","channels":{"_private.user_checkout":{"description":null,"bindings":{"kafka":{"envs":null,"partitions":1,"replicas":1,"configs":{"compression.type":"producer","leader.replication.throttled.replicas":"","message.downconversion.enable":"true","min.insync.replicas":"1","segment.jitter.ms":"0","cleanup.policy":"delete","flush.ms":"9223372036854775807","follower.replication.throttled.replicas":"","segment.bytes":"1073741824","retention.ms":"604800000","flush.messages":"9223372036854775807","message.format.version":"3.0-IV1","max.compaction.lag.ms":"9223372036854775807","file.delete.delay.ms":"60000","max.message.bytes":"1048588","min.compaction.lag.ms":"0","message.timestamp.type":"CreateTime","preallocate":"false","min.cleanable.dirty.ratio":"0.5","index.interval.bytes":"4096","unclean.leader.election.enable":"false","retention.bytes":"-1","delete.retention.ms":"86400000","segment.ms":"604800000","message.timestamp.difference.max.ms":"9223372036854775807","segment.index.bytes":"10485760"},"groupId":null,"schemaIdLocation":null,"schemaLookupStrategy":null,"bindingVersion":"unknown"}},"publish":null,"subscribe":null},"_public.user_signed_up":{"description":null,"bindings":{"kafka":{"envs":null,"partitions":1,"replicas":1,"configs":{"compression.type":"producer","leader.replication.throttled.replicas":"","message.downconversion.enable":"true","min.insync.replicas":"1","segment.jitter.ms":"0","cleanup.policy":"delete","flush.ms":"9223372036854775807","follower.replication.throttled.replicas":"","segment.bytes":"1073741824","retention.ms":"604800000","flush.messages":"9223372036854775807","message.format.version":"3.0-IV1","max.compaction.lag.ms":"9223372036854775807","file.delete.delay.ms":"60000","max.message.bytes":"1048588","min.compaction.lag.ms":"0","message.timestamp.type":"CreateTime","preallocate":"false","min.cleanable.dirty.ratio":"0.5","index.interval.bytes":"4096","unclean.leader.election.enable":"false","retention.bytes":"-1","delete.retention.ms":"86400000","segment.ms":"604800000","message.timestamp.difference.max.ms":"9223372036854775807","segment.index.bytes":"10485760"},"groupId":null,"schemaIdLocation":null,"schemaLookupStrategy":null,"bindingVersion":"unknown"}},"publish":null,"subscribe":null}}}
```

#

***

# Quickstart using docker on the local machine

Run a local kafka environment to manually test against. Note, security is not configured.

**Docker network**
> docker network create kafka_network


**ZooKeeper**
> docker run --name zookeeper -p 2181:2181 --network kafka_network -d zookeeper:latest
 
**Kafka**
> docker run --name kafka -p 9092:9092 --network kafka_network -e KAFKA_ZOOKEEPER_CONNECT=zookeeper:2181 -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092 -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT -e KAFKA_INTER_BROKER_LISTENER_NAME=PLAINTEXT -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 -d confluentinc/cp-kafka:latest
 
**Schema Registry**
>docker run --name schema-registry -p 8081:8081 --network kafka_network -e SCHEMA_REGISTRY_KAFKASTORE_BOOTSTRAP_SERVERS=kafka:9092 -e SCHEMA_REGISTRY_HOST_NAME=localhost -e SCHEMA_REGISTRY_LISTENERS=http://0.0.0.0:8081 -d confluentinc/cp-schema-registry:latest



## Confirm the containers running in the docker-network called 'kafka_network'
> docker network inspect kafka_network


## Test that it works...using kafka producer-consumer

Create a topic
> docker exec -it kafka /bin/bash -c "/usr/bin/kafka-topics --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic test"

List topics
> docker exec -it kafka /bin/bash -c "/usr/bin/kafka-topics --list --bootstrap-server kafka:9092"


*Notice that the --bootstrap-server parameter now points to kafka:9092 instead of localhost:9092, as the Kafka container is now referred to by its container name within the kafka_network network.* 

Produce messages
> docker exec -it kafka /bin/bash -c "/usr/bin/kafka-console-producer --broker-list kafka:9092 --topic test"

OR (own container rather than the 'kafka' container)

>  % docker run --name test-listing --network kafka_network -it  confluentinc/cp-kafka:latest  /bin/bash -c "/usr/bin/kafka-topics --list --bootstrap-server kafka:9092"

Consume messages
> docker exec -it kafka /bin/bash -c "/usr/bin/kafka-console-consumer --bootstrap-server kafka:9092 --topic test --from-beginning"






### **Cleanup time**

Stop the containers
>docker stop schema-registry\
>docker stop kafka\
>docker stop zookeeper
 
Remove the network
>docker network rm kafka_network


Remove containers
>docker rm schema-registry\
>docker rm kafka\
>docker rm zookeeper












