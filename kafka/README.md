# Kafka SpecMesh extension

Extends SpecMesh with the concept of public, protected and public topics. 

> Note: can be configured with preferred values instead of provided `public`. 
> 
> Use system properties `specmesh.public` --> `zzpublic` 

## Topic structure

Topic names in the spec come under the `channel` section. Their names either:

 * start with `_public`, `_protected` or `_private` for topics _owned_ by the domain, or
 * are the fully qualified topic name of some other (domain's) topic.
 
For topic's owned by the domain, their full topic name is that used in the spec, prefixed with the spec's domain id, 
i.e. `<domain-id>.<channel-name>`.  In this way, all topics owned by the domain and also prefixed by the domain name,
making it trivial to trace back the topic name to its owning domain. 

## Authorisation

## Group authorisation

The provisioner will set ACLs to allow the domain to use any consumer group prefixed with the domain id.
It is recommended that, in most cases, domain services use a consumer group name of `<domain-id>-<service-name>`.

## Transaction id authorisation

The provisioner will set ACLs to allow the domain to use any transaction id prefixed with the domain id. 

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
