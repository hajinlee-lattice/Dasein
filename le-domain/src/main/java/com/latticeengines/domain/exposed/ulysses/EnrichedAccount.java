package com.latticeengines.domain.exposed.ulysses;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import javax.persistence.Id;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class EnrichedAccount implements HasId<String> {

    public EnrichedAccount() {
        id = UUID.randomUUID().toString();
    }

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
    private String sourceAccountId;

    @JsonProperty
    @DynamoAttribute
    private Map<String, String> attributes = new HashMap<>();

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

    public String getSourceAccountId() {
        return sourceAccountId;
    }

    public void setSourceAccountId(String sourceAccountId) {
        this.sourceAccountId = sourceAccountId;
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
