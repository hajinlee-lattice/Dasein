package com.latticeengines.domain.exposed.ulysses;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Id;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class EnrichedAccount implements HasId<String> {

    @Id
    @JsonProperty
    @DynamoAttribute
    private String id;
    @Id
    @JsonProperty
    @DynamoAttribute
    private String latticeAccountId;
    @JsonProperty
    @DynamoAttribute
    private String tenantId;
    @JsonProperty
    @DynamoAttribute
    private String sourceId;
    @JsonProperty
    @DynamoAttribute
    private Map<String, String> attributes = new HashMap<>();

    // TODO Add LastModified

    public EnrichedAccount() {
        id = UUID.randomUUID().toString();
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public String getLatticeAccountId() {
        return latticeAccountId;
    }

    public void setLatticeAccountId(String latticeAccountId) {
        this.latticeAccountId = latticeAccountId;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public void setAttribute(String key, String value) {
        attributes.put(key, value);
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }
}
