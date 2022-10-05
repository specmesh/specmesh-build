package io.specmesh.apiparser.model;

public class Channel {
    enum PUBSUB {
        PUBLISH,
        SUBSCRIBE
    }

    private final String canonicalName;
    private final String name;
    private final PUBSUB pubsub;
    private final Operation operation;

    public Channel(final String name, final String canonicalName, final PUBSUB pubsub, final  Operation operation){
        this.name = name;
        this.canonicalName = canonicalName;
        this.pubsub = pubsub;
        this.operation = operation;
    }

    public String canonicalName() {
        return canonicalName;
    }

    public String name() {
        return name;
    }

    public PUBSUB pubsub() {
        return pubsub;
    }

    public Operation operation() {
        return operation;
    }
}
