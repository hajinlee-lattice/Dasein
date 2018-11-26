package com.latticeengines.domain.exposed.metadata.datastore;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "StorageType")
@JsonSubTypes({ @JsonSubTypes.Type(value = DynamoDataUnit.class, name = "Dynamo"),
        @JsonSubTypes.Type(value = RedshiftDataUnit.class, name = "Redshift"),
        @JsonSubTypes.Type(value = S3DataUnit.class, name = "S3"), })
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public abstract class DataUnit {

    @JsonProperty("Tenant")
    private String tenant;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("StorageType")
    public abstract StorageType getStorageType();

    public String getTenant() {
        return tenant;
    }

    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public enum StorageType {
        Dynamo, Hdfs, Redshift, S3
    }

}
