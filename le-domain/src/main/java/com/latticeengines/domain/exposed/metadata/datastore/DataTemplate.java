package com.latticeengines.domain.exposed.metadata.datastore;

import org.apache.avro.Schema;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class DataTemplate {
    @JsonProperty("MasterSchema")
    private Schema masterSchema;

    @JsonProperty("Tenant")
    private String tenant;

    @JsonProperty("Name")
    private String name;

    @JsonProperty("Entity")
    private BusinessEntity entity;

    public Schema getMasterSchema() {
        return masterSchema;
    }

    public void setMasterSchema(Schema masterSchema) {
        this.masterSchema = masterSchema;
    }

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

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

}
