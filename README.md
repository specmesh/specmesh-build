[![CI](https://github.com/specmesh/specmesh-build/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/specmesh/specmesh-build/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.specmesh/specmesh-kafka.svg)](https://central.sonatype.dev/search?q=specmesh-*)

# SpecMesh build tools

Enterprise Apache Kafka using AsyncAPI specs to build Data Mesh with GitOps


SpecMesh is an opinionated modelling layer over Apache Kafka resources that combines GitOps, AsyncAPI (modelling), a parser, testing, provisioning tools as well as chargeback support. By utilizing this methodology and toolset, it enables organizations to adopt Kafka at scale while incorporating simplification guardrails to prevent many typical mistakes. Resource provisioning is concealed beneath the AsyncAPI specification, providing a simplified view that allows both technical and non-technical users to design and build their Kafka applications as data products.

Links:
- [Source](https://github.com/specmesh/specmesh-build)
- [Wiki](https://github.com/specmesh/docs/wiki) 
- [CLI](cli/README.md)
- [SpecMesh.io](https://specmesh.github.io/site/)

Guides:
- [Apache Kafka Quick Start](https://github.com/specmesh/getting-started-apachekafka)
- More coming

**In 30 seconds.**

1. Write a spec for your BoundedContext (set of Kafka topics). They will all be prefixed with `acme.lifesyste.onboarding`. Set the access control, granting _private, _protected, _public access to this app (i.e. principal = acme.lifestyle.onboarding) and others. Grant restricted access on the `protected` topic to the `acme.finance.accounting` principal (it will have its own spec)

```yaml
asyncapi: '2.5.0'
id: 'urn:acme.lifestyle.onboarding'
info:
  title: ACME Lifestyle Onboarding
  version: '1.0.0'
  description: |
    The ACME lifestyle onboarding app that allows stuff - see this url for more detail.. etc

channels:
  _public.user_signed_up:
    bindings:
      kafka:
        partitions: 3
        replicas: 1
        configs:
          cleanup.policy: delete
          retention.ms: 999000

    publish:
      message:
        bindings:
          kafka:
            key:
              type: long
        payload:
          $ref: "/schema/simple.schema_demo._public.user_signed_up.avsc"

  _private.user_checkout:
    publish:
      message:
        bindings:
          kafka:
            key:
              $ref: "/schema/simple.schema_demo._public.user_checkout_key.yml"
        payload:
          $ref: "/schema/simple.schema_demo._public.user_checkout.yml"


  _protected.purchased:
    publish:
      summary: Humans purchasing food - note - restricting access to other domain principals
      tags:
        - name: "grant-access:acme.finance.accounting"
      message:
        name: Food Item
        tags: 
          - name: "human"
          - name: "purchase"
```


2. Provision this spec using the CLI. 

> % docker run --rm --network confluent -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli  provision -bs kafka:9092  -sr http://schema-registry:8081 -spec /app/simple_schema_demo-api.yaml -schemaPath /app
>
2.1. Topics created:
- acme.lifestyle.onboarding._public.user_signed_up
- acme.lifestyle.onboarding._private.user_checkout

2.2. Schema published:
- /schema/simple.schema_demo._public.user_signed_up.avsc
- /schema/simple.schema_demo._public.user_checkout_key.yml
- /schema/simple.schema_demo._public.user_checkout.yml

2.3. ACLs created:
- "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo._public, patternType=PREFIXED), entry=(principal=User:*, host=*, operation=READ, permissionType=ALLOW))",
- more

.


3. Check Storage metrics (chargeback)

> docker run --rm --network confluent -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli storage -bs kafka:9092 -spec /app/simple_spec_demo-api.yaml
>

```json
 {"acme.lifestyle.onboarding._public.user_signed_up":{"storage-bytes":1590,"offset-total":6},"acme.lifestyle._protected.purchased":{"storage-bytes":0,"offset-total":0},"acme.lifestyle._private.user_checkout":{"storage-bytes":9185,"offset-total":57}}
```
.

4. Check Consumption metrics (chargeback)

>%  docker run --rm --network confluent -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli consumption -bs kafka:9092 -spec /app/simple_spec_demo-api.yaml


```json
{"simple.spec_demo._public.user_signed_up":{"id":"some.other.app","members":[{"id":"console-consumer-7f9d23c7-a627-41cd-ade9-3919164bc363","clientId":"console-consumer","host":"/172.30.0.3","partitions":[{"id":0,"topic":"simple.spec_demo._public.user_signed_up","offset":57,"timestamp":-1}]}],"offsetTotal":57}}
```

## ACLs / Permissions

Notice how `_private`, `_public` or `_protected` is prefixed to the channel. This keyword can be altered in the following ways:
- it can be changed by passing the System.property as follows: `-Dspecmesh.public=everyone' -Dspecmesh.protected=some -Dspecmesh.private=mine`
- instead of 'inlining' the permission on the channel name, for example `_public.myTopic` - the permission can be controlled via channel.operation.tags see below for an example.

```yaml
channels:
  #  protected
  retail.subway.food.purchase:
    bindings:
      kafka:
    publish:
      tags: [
        name: "grant-access:some.other.domain.root"
      ]
```
```yaml
channels:
  #  public
  attendee:
    bindings:
    publish:
      tags: [
        name: "grant-access:_public"
      ]
```

## Schema References (AVRO)

Schema References are supported only by the Confluent Avro Serde. Common/Shared schemas are configured to be shared using the 'subject' as the full Record name (namespace+name). For example: com.example.shared.Currency - as opposed to the topic name (default schema subject). The way in which the schema is registered will follow the conventions defined below. It is configured as an attribute on the owning app.yml specificiation. As follows:
```yaml
        bindings:
          kafka:
            schemaIdLocation: "header"
            schemaLookupStrategy: "RecordNameStrategy"
            key:
              type: string
        payload:
          $ref: "/schema/com.example.shared.Currency.avsc"
```

More detail is provided below.

### **Confluent Schema Registry** conventions

https://docs.confluent.io/platform/6.2/schema-registry/serdes-develop/index.html#subject-name-strategy

**TopicNameStrategy** - Derives subject name from topic name. (This is the default.)

**RecordNameStrategy** - Derives subject name from record name, and provides a way to group logically related events that may have different data structures under a subject.

**TopicRecordNameStrategy** -	Derives the subject name from topic and record name, as a way to group logically related events that may have different data structures under a subject.


### **APICurio** conventions

https://github.com/asyncapi/bindings/blob/master/kafka/README.md
https://www.apicur.io/registry/docs/apicurio-registry/2.2.x/getting-started/assembly-using-kafka-client-serdes.html#registry-serdes-concepts-strategy_registry

**RecordIdStrategy** - Avro-specific strategy that uses the full name of the schema.

**TopicRecordIdStrategy** - Avro-specific strategy that uses the topic name and the full name of the schema.

**TopicIdStrategy** - Default strategy that uses the topic name and key or value suffix.

**SimpleTopicIdStrategy** - Simple strategy that only uses the topic name.

### Worked example

See code: kafka/src/test/resources/schema-ref (specs + schemas)

Note - the developers of the _com.example.trading-api.yml_ will be required to download a copy of the Currency avsc for the
development purposes, their spec is dependent upon the common (Currency) schema being available (published) in the
environment, otherwise the schema.provisioning process will fail because SchemaRegistry cannot resolve references upon being uploaded.


Source: com.example.trading-api.yml  (spec)

```yaml
  _public.trade:
    bindings:
      kafka:
        envs:
          - staging
          - prod
        partitions: 3
        replicas: 1
        configs:
          cleanup.policy: delete
          retention.ms: 999000

    publish:
      summary: Trade feed
      description: Doing clever things
      operationId: onTrade received
      message:
        bindings:
          kafka:
            schemaIdLocation: "header"
            key:
              type: string

        schemaFormat: "application/vnd.apache.avro+json;version=1.9.0"
        contentType: "application/octet-stream"
        payload:
          $ref: "/schema/com.example.trading.Trade.avsc"
```

Trade.avsc references the _Currency_ .avsc schema (the shared schema type)
```json
{
  "metadata": {
    "author": "John Doe",
    "description": "Schema for Trade data"
  },
  "type": "record",
  "name": "Trade",
  "namespace": "com.example.trading",
  "fields": [
    {
      "name": "id",  "type": "string",  "doc": "The unique identifier of the trade."
    },
    {
      "name": "detail", "type": "string",  "doc": "Trade details."
    },
    {
      "name": "currency",
      "type": "com.example.shared.Currency",
      "subject": "com.example.shared.Currency",
      "doc": "Currency is from another 'domain'."
    }
  ]
}
```

Share schemas are published by the owner. The spec: com.example.shared-api.yml will
- reference the 'owned' schemas for publishing
- specify the _schemaLookupStrategy_: "RecordNameStrategy"

**RecordNameStrategy** sets the _schema-subject_ as the full record name: _com.example.shared.Currency_. 
If not used then the default rules apply above (i.e. topic-name '-' - value) and it cannot be shared

Below: com.example.shared-api.yml (api spec) 
```yaml
asyncapi: '2.4.0'
id: 'urn:com.example.shared'
info:
  title: Common Data Set
  version: '1.0.0'
  description: |
    Common data set - schema reference example
servers:
  mosquitto:
    url: mqtt://test.mosquitto.org
    protocol: kafka
channels:
  _public.currency:
    bindings:
      kafka:
        partitions: 3
        replicas: 1
        configs:
          cleanup.policy: delete
          retention.ms: 999000
    publish:
      summary: Currency things
      operationId: onCurrencyUpdate
      message:
        schemaFormat: "application/vnd.apache.avro+json;version=1.9.0"
        contentType: "application/octet-stream"
        bindings:
          kafka:
            schemaIdLocation: "header"
            schemaLookupStrategy: "RecordNameStrategy"
            key:
              type: string
        payload:
          $ref: "/schema/com.example.shared.Currency.avsc"
```

# Scaling Kafka resources in non-production environments

It's not uncommon for non-production environments to have less resources available for running services. 
If your lower environments have smaller Kafka clusters and data volumes, then you may wish to provision topics with lower retention and less partitions & replicas.
(This can be particularly relevant when running Kafka within some cloud platforms where partitions are limited or have an associated cost).

Scaling cluster resources can be achieved by creating per-environment specs. However, it is also possible to roll out the same spec and scale resources as explained below.

## Reducing topic partitions

The `--partition-count-factor` command line parameter and `Provisioner.partitionCountFactor()` method can be used to apply a factor to scale _down_ the partition counts of a spec.

For example, if channel/topic has 20 partitions in the spec, provisioning with either `--partition-count-factor 0.1` or `Provisioner.builder().partitionCountFactor(0.1)`
will provision the topic with `20 x0 0.1`, i.e. 2 partitions.

Note: topics with multiple partitions will _always_ provision with at least 2 partitions. Only topics with a single partition in the spec will have one partition, regardless of the factor applied.
This is to ensure partitioning related bugs and issues can be detected in all environments.

For example, a dev cluster can set `--partition-count-factor = 0.00001` to ensure all topics have either 1 or 2 topics, other non-prod clusters `--partition-count-factor = 0.1` to save resources and staging and prod would leave the default `--partition-count-factor = 1`.

## Reducing topic replicas

It is recommended that `replicas` is _not_ set in the spec in most cases. Instead, set the cluster-side `default.replication.factor` config as needed. 

For example, a single-node dev cluster can set `default.replication.factor = 1`, other non-prod clusters `default.replication.factor = 3` and staging and prod may require `default.replication.factor = 4`.

## Reducing topic retention

It is recommended that the `retention.ms` topic config is _not_ set in the spec in most cases. Instead, set the cluster-side `log.retention.ms` (or related) config as needed.
This allows the cluster, as a whole, to control the _default_ log retention. Only topics that require a specific log retention to meet business requirements need have their retention set in the spec,
overriding the default.

# Developer Notes

1. Install the intellij checkstyle plugin and load the config from config/checkstyle.xml
1. build using: `./gradlew`
