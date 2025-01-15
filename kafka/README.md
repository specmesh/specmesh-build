# Kafka SpecMesh extension

Extends SpecMesh with the concept of public, protected and public topics. 

> Note: can be configured with preferred values instead of provided `public`. 
> 
> Use system properties `specmesh.public` --> `zzpublic` 

## Topic structure

Topic names in the spec come under the `channel` section. Their names either:

 * start with `_public`, `_protected` or `_private` for topics _owned_ by the domain. Sharing can also be specified using 'grant-access:' at the cost of explicit sharing 
 * are the fully qualified topic name of some other (domain's) topic.
 
For topic's owned by the domain, their full topic name is that used in the spec, prefixed with the spec's domain id, 
i.e. `<domain-id>.<channel-name>`.  In this way, all topics owned by the domain and also prefixed by the domain name,
making it trivial to trace back the topic name to its owning domain. 

### Working with Kafka Streams internal topics

Kafka Streams applications can create internal repartition and changelog topics.
These internal topics should be defined as channels in the mesh spec.

At the time of writing the names of internal topics are restricted to the following format by Kafka Streams:

* Change log topics: `${application.id"}-${store.name}-changelog`
* Repartition topics: `${application.id"}-${node.name}-repartition`

If you wish to maintain the visibility marker for internal topics, then it is recommended the `application.id"`, passed
in the Kafka Streams properties, should be set to `${domain.id}._private.${service.name}`. 
This ensures the ACLs are correctly set for the private internal topics of the service.  

Optionally, the Kafka `client.id` can be set to `${domain.id}.${service.name}`, to remove the `_private` component, if required.
Unfortunately, `group.id` can not be customised in streams apps, hence the `_private` component is unavoidable. 

Chose store names and repartition node names so that they build the channel and topic names required. 

#### Changelog example

Given an `application.id` in the form `${domain.id}._private.${service.name}`, for example `acme.user._private.user-service`,
and given a store name of `store.user`, for example:

```java
    streamBuilder.addStateStore(
       Stores.keyValueStoreBuilder(
           Stores.inMemoryKeyValueStore("store.user"),
           storeKeySerde,
           storeValSerde)
     .withLoggingEnabled(logConfig));
```

The topology will require the spec to define a channel named `_private.user-service-store.user-changelog`, which will 
provision a topic named `acme.user._private.user-service-store.user-changelog`.

#### Repartition example

Given an `application.id` in the form `${domain.id}._private.${service.name}`, for example `acme.user._private.user-service`,
and given a node name of `rekey.by-age`, for example:

```java
 users.map((k, v) -> new KeyValue<>(v.getAge(), 1), Named.as("select-key"))
    .groupByKey(
      Grouped.<Integer, Integer>as("rekey.by-age")
              .withKeySerde(Serdes.Integer())
              .withValueSerde(Serdes.Integer()))
    .count(Named.as("count-by-age"));
```

The topology will require the spec to define a channel named `_private.client-func-demo-rekey.by-age-repartition`, which will
provision a topic named `acme.user._private.client-func-demo-rekey.by-age-repartition`.

## Authorisation

### Resource types

#### Group authorisation

The provisioner will set ACLs to allow the domain to use any consumer group prefixed with the domain id.
It is recommended that, in most cases, domain services use a consumer group name of `<domain-id>-<service-name>`.

#### Transaction id authorisation

The provisioner will set ACLs to allow the domain to use any transaction id prefixed with the domain id. 

#### Topic authorisation

The provisioner will set ACLs to enforce the following topic authorisation:

#### Public topics:

Only the domain itself can `WRITE` to public topics; Any domain can `READ` from them.

#### Protected topics:

Only the domain itself can `WRITE` to protected topics; Any domain specifically tagged in the spec can `READ` from them.

Access to a protected topic can be granted to another domain by adding a `grant-access:` tag to the topic:

```yaml
      tags: [
        name: "grant-access:some.other.domain"
      ]
```

#### Private topics:

Only the domain itself can `WRITE` to private topics; Only the domain itself can `READ` to private topics.

Additionally, the domain itself also has `CREATE` permissions for topics under its domain id. 
This allows the domain's services to create any additional internal topics required, e.g. Kafka Streams 
library creates repartition and changelog topics for stores automatically using the Admin client.

As services are free to make additional private topics, provisioning does _not_ remove existing private topics not in the spec.
This places the responsibility of cleaning up private topics on engineering teams. However, as these are private
topics, it is easy to determine if such topics are or are not actively in use by the domain's services.

### Looking inside ACLs - whats really going on

Too many ACLs can affect cluster performance. Look at the set of ACLs below.  The domain owner `simple.provision_demo` has access to everything (CREATE, READ, WRITE, DESCRIBE) below its designated topic prefix -  which is also its domain name. Notice how there are READ, DESCRIBE ACls for all _public topic. Protected /`_protected` topics require a set of ACLs for each topic using the 'LITERAL' pattern type - for each 'grant' to another domain owner `User:some.other.domain.acme-A` there will be a set of ACLs created.


The set of ACLs created from the `provisioner-functional-test-api.yaml`
```text
[(pattern=ResourcePattern(resourceType=GROUP, name=simple.provision_demo, patternType=PREFIXED)
=(principal=User:simple.provision_demo, host=*, operation=READ, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TOPIC, name=simple.provision_demo._private, patternType=PREFIXED)
=(principal=User:simple.provision_demo, host=*, operation=CREATE, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TRANSACTIONAL_ID, name=simple.provision_demo, patternType=PREFIXED)
=(principal=User:simple.provision_demo, host=*, operation=WRITE, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TRANSACTIONAL_ID, name=simple.provision_demo, patternType=PREFIXED)
=(principal=User:simple.provision_demo, host=*, operation=DESCRIBE, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TOPIC, name=simple.provision_demo, patternType=PREFIXED)
=(principal=User:simple.provision_demo, host=*, operation=WRITE, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TOPIC, name=simple.provision_demo, patternType=PREFIXED)
=(principal=User:simple.provision_demo, host=*, operation=DESCRIBE, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TOPIC, name=simple.provision_demo, patternType=PREFIXED)
=(principal=User:simple.provision_demo, host=*, operation=READ, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TOPIC, name=simple.provision_demo._public, patternType=PREFIXED)
=(principal=User:*, host=*, operation=READ, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TOPIC, name=simple.provision_demo._public, patternType=PREFIXED)
=(principal=User:*, host=*, operation=DESCRIBE, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TOPIC, name=simple.provision_demo._protected.user_info, patternType=LITERAL)
=(principal=User:some.other.domain.acme-A, host=*, operation=READ, permissionType=ALLOW)), 

(pattern=ResourcePattern(resourceType=TOPIC, name=simple.provision_demo._protected.user_info, patternType=LITERAL)
=(principal=User:some.other.domain.acme-A, host=*, operation=DESCRIBE, permissionType=ALLOW))

(pattern=ResourcePattern(resourceType=CLUSTER, name=kafka-cluster, patternType=PREFIXED)
=(principal=User:simple.provision_demo, host=*, operation=IDEMPOTENT_WRITE, permissionType=ALLOW)), 
]
```

### Custom usernames

The normal SpecMesh convention is for domain users to have the same name as the domain id.
For example, given a spec with `id: 'urn:.london.hammersmith.olympia.bigdatalondon'`, SpecMesh will create ACLs to for a principle with the name `london.hammersmith.olympia.bigdatalondon`.

Some times the name of the principle is not in your control, e.g. at the time of writing, service accounts in Confluent Cloud have a user-defined name, but the _principle_ that Kafka sees is the service account _id_, not name.  
The Id is system generated, e.g. `sa-dkg9sfd`.

In Confluent Cloud, it is recommended that a service account is created with the domain id as the name, and the account id is passed to specmesh via:

1. If using the [command line](../cli/README.md), use the `--domain-user` command line parameter. E.g. `--domain-user=sa-dkg9sfd`
2. If using the [`Provisioner` class](src/main/java/io/specmesh/kafka/provision/Provisioner.java), use the `domainUserAlias` method. E.g
   ```
   Provisioner.builder()
      .domainUserAlias("sa-dkg9sfd")
   ```   