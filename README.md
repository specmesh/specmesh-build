# SpecMesh build tools

Provides:
1. AsyncAPI spec parser
1. Terraform JavaTestUnit to provision and assert expected resources
1. Kafka Topics provisioning. Per environment partition-count, replica-count, retention in days. Topics are fully qualified using snake_case
1. Kafka ACL provisioning restricting client.id access so that only owners can
1. Validate Topic names must contain public, private or protected
1. GradlePlugin to run Terraform provisioning or resources as part of any infrastructure pipeline (demo included)