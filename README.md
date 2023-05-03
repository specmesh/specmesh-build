[![CI](https://github.com/specmesh/specmesh-build/actions/workflows/build.yml/badge.svg?branch=main)](https://github.com/specmesh/specmesh-build/actions/workflows/ci.yml)
[![Maven Central](https://img.shields.io/maven-central/v/io.specmesh/specmesh-kafka.svg)](https://central.sonatype.dev/search?q=specmesh-*)

# SpecMesh build tools

SpecMesh is an opinionated layer over Apache Kafka resources that combines GitOps, AsyncAPI, a parser, and provisioning tools. By utilizing a methodology and toolset, it enables organizations to adopt Kafka at scale while incorporating guardrails to prevent many typical mistakes. Resource provisioning is concealed beneath the AsyncAPI specification, providing a simplified view that allows both technical and non-technical users to design and build their Kafka applications as data products.

For more information see [What Why Who](what-why-who.md)


# Adoption
SpecMesh is designed for users (or applications) that are eager to build new Apache Kafka apps using this approach (resource structuring/modelling). Existing apps that do not conform to the required structure captured in the async api spec cannot be adapted at this time; however, we have plans to support such apps in the future.

# Compared to?
- Ansible & Terraform: Are DevOps tools - they maintain their own state of provisioned resources against which their execution will check as the scripts are run. It is generally advisable to use Ansible and Terraform for Server-like infrastructure (i.e. Brokers, Load balancers, Firewalls) as opposed to App-like resources (data infra - such as topics where GitOps is more suitable)
- JulieOps: Has similar traits to SpecMesh, and while there is some overlap (both use GitOps and topic structure), the philosophy is different. SpecMesh is a data modeling abstraction layer, whereas JulieOps doesn't hide the underlying resources. They are not conflated; instead, they are configured independently (ACLs, topics, schemas). JulieOps also extends somewhat into server-side infra provisioning when such as connectors and ksqldb. 


# Developer Notes

1. Install the intellij checkstyle plugin and load the config from config/checkstyle.xml