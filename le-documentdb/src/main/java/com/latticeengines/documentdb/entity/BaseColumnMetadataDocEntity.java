package com.latticeengines.documentdb.entity;

import javax.persistence.Column;
import javax.persistence.MappedSuperclass;

import org.hibernate.annotations.Type;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

@MappedSuperclass
abstract class BaseColumnMetadataDocEntity extends BaseMultiTenantDocEntity implements MetadataEntity {

    @JsonProperty("Metadata")
    @Type(type = JSON_TYPE)
    @Column(name = "Metadata", columnDefinition = JSON_COLUMN)
    private ColumnMetadata metadata;

    public ColumnMetadata getMetadata() {
        return metadata;
    }

    public void setMetadata(ColumnMetadata metadata) {
        this.metadata = metadata;
    }
}
