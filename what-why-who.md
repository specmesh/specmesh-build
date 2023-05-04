
# The problem

[Apache Kafka](https://kafka.apache.org/) is not opinionated, which means you can:

- Create topics with any name, using any structure or capitalization (camel case, kebab case, lower/upper case). This lack of constraint often results in flat structures of topic names that have unclear ownership.
- Experience confusing or non-existent governance; associating and managing ACLs is complicated by the fact that consumer IDs, topic names, and ACLs are all managed separately.
- Encounter inconsistent tooling in many areas. Some apps use the Java admin client, while DevOps tools may include Ansible, Terraform, or JulieOps (GitOps) for provisioning resources.

Adopting Kafka is hard; adopting Kafka at scale is even harder. It's difficult to make the right decisions early on, and often things become hidden, lost, or misunderstood.

# The solution
SpecMesh provides an opinionated way of designing and modeling your Kafka resources together. It adds an opinionated layer over Apache Kafka resources, modeling and conflating resources such as topics, schemas, and ACLs using AsyncAPI. Although AsyncAPI already supports Kafka for modeling resources like topics, SpecMesh extends this model to include:

- GitOps semantics: Provisioning of topics, schemas, and ACLs (with git maintaining the current and next state, as well as API spec history)
- Hierarchical structural modeling of topic names: i.e., `a.b.c`
- DDD-Aggregate Identification introduction to enforce resource ownership over all resources
- Conflation of topic governance using well-understood concepts by demarcating them as `public`, `private`, or `protected`
- Self-governance enablement for `protected` topics by using the `grant:a.b.c.` tag metadata
- Inclusion of a rich metadata modeling overlay to document attributes, making them suitable for governance, data cataloging, or building extended functionality


# A sample specification #


```yaml
asyncapi: '2.5.0'
id: 'urn:simple:spec_demo'
info:
  title: User flow modelling
  version: '1.0.0'
  description: |
    An app that models user behaviour and events
channels:
  _public/user_signed_up:
    bindings:
      kafka:
        envs:
          - staging
          - prod
        partitions: 3
        replicas: 1
        retention: 1
        configs:
          cleanup.policy: delete

    publish:
      summary: Inform about signup
      operationId: onSignup
      message:
        bindings:
          kafka:
            schemaIdLocation: "payload"
        schemaFormat: "application/vnd.apache.avro+json;version=1.9.0"
        contentType: "application/octet-stream"
        payload:
          $ref: "/schema/simple.schema_demo._public.user_signed_up.avsc"

  _private/user_checkout:
    bindings:
      kafka:
        envs:
          - staging
          - prod
        partitions: 3
        replicas: 1
        retention: 1
        configs:
          cleanup.policy: delete

    publish:
      summary: User purchase confirmation
      operationId: onUserCheckout
      message:
        bindings:
          kafka:
            schemaIdLocation: "payload"
        schemaFormat: "application/json;version=1.9.0"
        contentType: "application/json"
        payload:
          $ref: "/schema/simple.schema_demo._public.user_checkout.yml"


  _protected/purchased:
    bindings:
      kafka:
        partitions: 3

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
        payload:
          type: object
          properties:
            id:
              type: integer
              minimum: 0
              description: Id of the food
            cost:
              type: number
              minimum: 0
              description: GBP cost of the food item
            human_id:
              type: integer
              minimum: 0
              description: Id of the human purchasing the food

  # SUBSCRIBER WILL REQUEST SCHEMA from SR and CodeGen required classes. Header will be used for Id
  /london/hammersmith/transport/_public/tube:
    subscribe:
      summary: Humans arriving in the borough
      bindings:
        kafka:
          groupId: 'aConsumerGroupId'
      message:
        schemaFormat: "application/vnd.apache.avro+json;version=1.9.0"
        contentType: "application/octet-stream"
        bindings:
          kafka:
            schemaIdLocation: "header"
            key:
              type: string
        payload:
          # client should lookup this schema remotely from the schema registry - it is owned by the publisher
          $ref: "london.hammersmith.transport._public.tube.passenger.avsc"
```

**Noteworthy features from the above spec**
- hierarchy: The 'app-name, aggregate' is called 'simple:spec_demo' - it prefixes topics owned by this app. ACLs will also be provisioned to prevent other principles for accessing 'private' data
- absolute and relative paths: topics owned by this app (channels section) start with 'private, public or protected' - the convention is used to combine topic naming structure with permissions like those of a file system.  
- Absolute paths indicate consumption from other apps. i.e.  `/london/hammersmith/transport/`

Next: Look at `How it works`