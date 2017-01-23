package com.latticeengines.domain.exposed.metadata;

import java.util.List;

import javax.persistence.DiscriminatorValue;
import javax.persistence.Entity;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@Entity
@JsonIgnoreProperties({ "hibernateLazyInitializer", "handler" })
@DiscriminatorValue("HDFS")
public class HdfsStorage extends StorageMechanism {

    @JsonIgnore
    public List<Extract> getExtracts() {
        return getTable().getExtracts();
    }
}
