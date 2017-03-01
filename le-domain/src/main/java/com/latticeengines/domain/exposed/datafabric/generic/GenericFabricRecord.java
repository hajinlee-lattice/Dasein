package com.latticeengines.domain.exposed.datafabric.generic;

import javax.persistence.Id;

import org.apache.avro.generic.GenericRecord;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class GenericFabricRecord implements  HasId<String> {

    @Id
    private String id;
    @JsonProperty("GenericRecord")
    private GenericRecord genericRecord;

    public GenericRecord getGenericRecord() {
        return genericRecord;
    }

    public void setGenericRecord(GenericRecord genericRecord) {
        this.genericRecord = genericRecord;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    
}

