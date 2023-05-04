[![CI](https://github.com/specmesh/specmesh-build/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/specmesh/specmesh-build/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.specmesh/specmesh-kafka.svg)](https://central.sonatype.dev/search?q=specmesh-*)

# SpecMesh build tools

SpecMesh is an opinionated modelling layer over Apache Kafka resources that combines GitOps, AsyncAPI (modelling), a parser, testing and provisioning tools. By utilizing this methodology and toolset, it enables organizations to adopt Kafka at scale while incorporating simplification guardrails to prevent many typical mistakes. Resource provisioning is concealed beneath the AsyncAPI specification, providing a simplified view that allows both technical and non-technical users to design and build their Kafka applications as data products.

For more information see [What Why Who](what-why-who.md)

And one place to look at everything, here is a simple SpecMesh API spec.
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
        retention: 1
        configs:
          cleanup.policy: delete

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

# Best Practice
SpecMesh enables the following best practices
1. Topic structures that reflect ownership using Aggregate/AppId concepts
1. Clearly named and configured topic properties (partitioning, replication retention etc)
1. [GitOps](https://www.redhat.com/en/topics/devops/what-is-gitops) as the source of truth for configuration and change management
1. Simplified, self-governance modelling by prefixing topic `_private`, `_protected`,`_public` concepts. i.e. `acme:lifestyle.onboarding._public.user_signed_up`
1. Systematic & consistent provisioning of structured topics and their associated Kafka resources
1. Industry standard based provisioning process
1. Combining disparate resources into a simplified, yet logical model (topics, schemas, ACLs)
1. Prevention of incorrectly named topics or other resources
1. Bundling together sets of topics into a single, publishable API that [models well known concepts](https://www.asyncapi.com/docs/tutorials/getting-started/event-driven-architectures)
1. Support for metadata modelling and attachment ([AsyncAPI tags](https://www.asyncapi.com/docs/reference/specification/v2.4.0#a-nametagsobjectatags-object))

# Want to get started? 
Look at the [Get Going Guide](getgoingguides.md)

# Adoption
SpecMesh is designed for users (or applications) that are eager to build new Apache Kafka apps using this approach (resource structuring/modelling). Existing apps that do not conform to the required structure captured in the AsyncAPI spec cannot be adapted at this time; however, we have plans to support such apps in the future.

# Compared to?
- **[Ansible](https://www.ansible.com/) & [Terraform](https://developer.hashicorp.com/terraform/docs)** are [DevOps](https://www.atlassian.com/devops) tools - they maintain their own state of provisioned resources against which their execution will check as the scripts are run. It is generally advisable to use Ansible and Terraform for Server-like infrastructure (i.e. Brokers, Load balancers, Firewalls) as opposed to App-like resources (data infra - such as topics where GitOps is more suitable). Confluent support both [Ansible](https://docs.confluent.io/ansible/current/overview.html) and [Terraform](https://docs.confluent.io/cloud/current/clusters/terraform-provider.html).
- **[JulieOps](https://julieops.readthedocs.io/en/3.x/)** is a Kafka [GitOps](https://www.cloudbees.com/gitops/what-is-gitops) tool with similarities to SpecMesh; both utilize GitOps to provision and maintain Kafka resources (Topics etc). However, their underlying philosophies differ. SpecMesh serves as a data modeling abstraction layer based upon the [AsyncAPI](https://www.asyncapi.com/), while JulieOps does not hide or model the underlying resources. Instead of being conflated, they are independently configured, including ACLs, topics, and schemas. JulieOps also extends into server-side infrastructure provisioning, such as connectors and ksqlDB. 


# GitOps versus DevOps

It is not accurate to compare GitOps and DevOps as if they were in opposition, as they are not competing methodologies. Instead, GitOps is a specific approach within the broader DevOps philosophy.

DevOps is a set of practices that combines software development (Dev) and IT operations (Ops) to shorten the development life cycle and deliver high-quality software continuously. It emphasizes collaboration, automation, and integration between developers and IT operations teams.

GitOps, on the other hand, is a specific implementation of DevOps that uses Git as the single source of truth for infrastructure and application code. It relies on declarative infrastructure and version control to manage and deploy applications and infrastructure. GitOps allows for easier collaboration, automation, and rollback capabilities by treating infrastructure as code.

In summary, GitOps is a subset of the DevOps methodology. Therefore, it is more appropriate to consider how GitOps can enhance your existing DevOps processes rather than choosing one over the other.


# Developer Notes

1. Install the intellij checkstyle plugin and load the config from config/checkstyle.xml