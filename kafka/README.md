# Kafka SpecMesh extension

Extends SpecMesh with the concept of public, protected and public topics.

## Authorisation

## Topic authorisation

The provisioner will set ACLs to enforce the following topic authorisation:

### Public topics:

Only the domain itself can `WRITE` to public topics; Any domain can `READ` from them.

### Protected topics:

Only the domain itself can `WRITE` to protected topics; Any domain specifically tagged in the spec can `READ` from them.

Access to a protected topic can be granted to another domain by adding a `grant-access:` tag to the topic:

```yaml
      tags: [
        name: "grant-access:some.other.domain"
      ]
```

### Private topics:

Only the domain itself can `WRITE` to private topics; Only the domain itself can `READ` to private topics.

Additionally, the domain itself also has `CREATE` permissions for topics under its domain id. 
This allows the domain's services to create any additional internal topics required, e.g. Kafka Streams 
library creates repartition and changelog topics for stores automatically using the Admin client.

As services are free to make additional private topics, provisioning does _not_ remove existing private topics not in the spec.
This places the responsibility of cleaning up private topics on engineering teams. However, as these are private
topics, it is easy to determine if such topics are or are not actively in use by the domain's services.
