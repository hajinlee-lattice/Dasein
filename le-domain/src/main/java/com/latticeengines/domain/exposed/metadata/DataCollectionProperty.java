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

    public DataCollection getOwner() {
        return owner;
    }

    public void setOwner(DataCollection owner) {
        this.owner = owner;
    }

    @JsonIgnore
    @Override
    public Class<DataCollection> getOwnerEntity() {
        return DataCollection.class;
    }

}
