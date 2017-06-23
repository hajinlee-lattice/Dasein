package com.latticeengines.domain.exposed.metadata;

import javax.persistence.Entity;
import javax.persistence.JoinColumn;
import javax.persistence.ManyToOne;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.latticeengines.domain.exposed.dataplatform.HasPid;
import com.latticeengines.domain.exposed.db.HasOptionAndValue;

@Entity
@javax.persistence.Table(name = "METADATA_DATA_COLLECTION_PROPERTY")
public class DataCollectionProperty extends MetadataProperty<DataCollection> implements HasOptionAndValue, HasPid {

    public DataCollectionProperty(){}

    public DataCollectionProperty(String property, String value) {
        super(property, value);
    }

    @ManyToOne
    @JoinColumn(name = "FK_DATA_COLLECTION_ID", nullable = false)
    @JsonIgnore
    private DataCollection owner;

    @Override
    @JsonIgnore
    public DataCollection getOwner() {
        return owner;
    }

    @Override
    @JsonIgnore
    public void setOwner(DataCollection owner) {
        this.owner = owner;
    }

}
