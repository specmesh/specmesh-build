package io.specmesh.apiparser.model;

import lombok.Value;
import lombok.experimental.Accessors;

@Value
@Accessors(fluent=true)
public class SchemaInfo {
    private final String schemaRef;
    private final String schemaFormat;
    private final String schemaIdLocation;
    private String schemaIdPayloadLocation;
    private final String contentType;
    private String schemaLookupStrategy;

    public SchemaInfo(final String schemaRef, final String schemaFormat, final String schemaIdLocation,
                      final String schemaIdPayloadLocation, final String contentType, final String schemaLookupStrategy) {
        this.schemaRef = schemaRef;
        this.schemaFormat = schemaFormat;
        this.schemaIdLocation = schemaIdLocation;
        this.schemaIdPayloadLocation = schemaIdPayloadLocation;
        this.contentType = contentType;
        this.schemaLookupStrategy = schemaLookupStrategy;
    }
}
