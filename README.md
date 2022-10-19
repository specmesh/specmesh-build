# SpecMesh build tools [![CI](https://github.com/specmesh/specmesh-build/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/specmesh/specmesh-build/actions/workflows/ci.yml)  [![Release Builds](https://github.com/specmesh/specmesh-build/actions/workflows/release.yml/badge.svg?branch=main)](https://github.com/specmesh/specmesh-build/actions/workflows/release.yml)

Provides:
1. AsyncAPI spec parser
1. Terraform JavaTestUnit to provision and assert expected resources
1. Kafka Topics provisioning. Per environment partition-count, replica-count, retention in days. Topics are fully qualified using snake_case
1. Kafka ACL provisioning restricting client.id access so that only owners can write to private and anyone to public. 
1. Validate Topic names must contain public, private or protected.
1. GradlePlugin to run Terraform provisioning or resources as part of any infrastructure pipeline (demo included).
1. Provision schemas to specified Schema registry. From spec defined SR cluster that is identified with SR- prefix.


# Developer Notes

1. Install the intellij checkstyle plugin and load the config from config/checkstyle.xml