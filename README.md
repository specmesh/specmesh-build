[![CI](https://github.com/specmesh/specmesh-build/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/specmesh/specmesh-build/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.specmesh/specmesh-kafka.svg)](https://central.sonatype.dev/search?q=specmesh-*)

# SpecMesh build tools

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
id: 'urn:acme:lifestyle:onboarding'
info:
  title: ACME Lifestyle Onboarding
  version: '1.0.0'
  description: |
    The ACME lifestyle onboarding app that allows stuff - see this url for more detail.. etc

channels:
  _public/user_signed_up:
    bindings:
      kafka:
        partitions: 3
        replicas: 1
        configs:
          cleanup.policy: delete
          retention.ms: 999000

    publish:
      message:
        payload:
          $ref: "/schema/simple.schema_demo._public.user_signed_up.avsc"

  _private/user_checkout:
    publish:
      message:
        payload:
          $ref: "/schema/simple.schema_demo._public.user_checkout.yml"


  _protected/purchased:
    publish:
      summary: Humans purchasing food - note - restricting access to other domain principals
      tags: [
        name: "grant-access:.acme.finance.accounting"
      ]
      message:
        name: Food Item
        tags: [
          name: "human",
          name: "purchase"
        ]
```

2. Provision this spec using the CLI. 

> % docker run --rm --network confluent -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli  provision -bs kafka:9092  -sr http://schema-registry:8081 -spec /app/simple_schema_demo-api.yaml -schemaPath /app
>
2.1. Topics created:
- acme.lifestyle.onboarding._public.user_signed_up
- acme.lifestyle.onboarding._private.user_checkout

2.2. Schema published:
- /schema/simple.schema_demo._public.user_signed_up.avsc

2.3. ACLs created:
- "name" : "(pattern=ResourcePattern(resourceType=TOPIC, name=simple.spec_demo._public, patternType=PREFIXED), entry=(principal=User:*, host=*, operation=READ, permissionType=ALLOW))",
- more

.


3. Check Storage metrics (chargeback)

> docker run --rm --network confluent -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli storage -bs kafka:9092 -spec /app/simple_spec_demo-api.yaml
>

```json
 {"acme.lifestyle.onboarding._public.user_signed_up":{"storage":1590,"offset-total":6},"acme.lifestyle._protected.purchased":{"storage":0,"offset-total":0},"acme.lifestyle._private.user_checkout":{"storage":9185,"offset-total":57}}
```
.

4. Check Consumption metrics (chargeback)

>%  docker run --rm --network confluent -v "$(pwd)/resources:/app" ghcr.io/specmesh/specmesh-build-cli consumption -bs kafka:9092 -spec /app/simple_spec_demo-api.yaml


```json
{"simple.spec_demo._public.user_signed_up":{"id":"some.other.app","members":[{"id":"console-consumer-7f9d23c7-a627-41cd-ade9-3919164bc363","clientId":"console-consumer","host":"/172.30.0.3","partitions":[{"id":0,"topic":"simple.spec_demo._public.user_signed_up","offset":57,"timestamp":-1}]}],"offsetTotal":57}}
```


# Developer Notes

1. Install the intellij checkstyle plugin and load the config from config/checkstyle.xml