package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;
import javax.persistence.Table;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasOptionAndValue;

@Entity
@Table(name = "METADATA_SEGMENT_PROPERTY")
public class MetadataSegmentProperty extends MetadataProperty<MetadataSegment> implements HasOptionAndValue, HasPid {

    public MetadataSegmentProperty(){}

    public MetadataSegmentProperty(String property, String value) {
        super(property, value);
    }

    @ManyToOne
    @JoinColumn(name = "METADATA_SEGMENT_ID", nullable = false)
    @JsonIgnore
    private MetadataSegment owner;

    @Override
    @JsonIgnore
    public MetadataSegment getOwner() {
        return owner;
    }

    @Override
    @JsonIgnore
    public void setOwner(MetadataSegment owner) {
        this.owner = owner;
    }

}
