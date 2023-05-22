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

Here is a simple SpecMesh API spec. When provision is executed, `Topics` will be provisioned for each app.id + channel - i.e. acme.lifestyle.onboarding._public.user_signed_up. In this case, the domain owner (app-id) is the only principle with ACL permissions to write to it, however, all principles can consume from the `_public` topic. ACLs are implied through the `_private`, `_protected`, `_public` convention. The structural requirement enforces clear topic and acl ownership (including schemas), but also means the ecosytem resources can not be mined to extract `storage` and `consumption` metrics for billing purposes.
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
      summary: Humans purchasing food - note - restricting access to other domain principles
      tags: [
        name: "grant-access:.some.other.domain.root"
      ]
      message:
        name: Food Item
        tags: [
          name: "human",
          name: "purchase"
        ]
```


# Developer Notes

1. Install the intellij checkstyle plugin and load the config from config/checkstyle.xml