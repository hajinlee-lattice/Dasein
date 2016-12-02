package com.latticeengines.domain.exposed.ulysses;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.persistence.Id;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datafabric.DynamoAttribute;
import com.latticeengines.domain.exposed.dataplatform.HasId;

public class EnrichedAccount implements HasId<String> {

    @Id
    @JsonProperty("latticeRequestId")
    private String id;

    @JsonProperty("score")
    @DynamoAttribute("score")
    private double score;

    @JsonProperty("attributes")
    private Map<String, String> attributes = new HashMap<>();

    @JsonProperty("campaignIds")
    @DynamoAttribute("campaignIds")
    private List<String> campaignIds = new ArrayList<>();

    @JsonProperty("tenantId")
    @DynamoAttribute("tenantId")
    private String tenantId;

    @JsonProperty("externalId")
    @DynamoAttribute("externalId")
    private String externalId;

    @JsonProperty("requestTimestamp")
    @DynamoAttribute("requestTimestamp")
    private long requestTimestamp;

    public void setCampaignIds(List<String> campaignIds) {
        this.campaignIds = campaignIds;
    }

    public List<String> getCampaignIds() {
        return campaignIds;
    }

    @Override
    public String getId() {
        return id;
    }

    @Override
    public void setId(String id) {
        this.id = id;
    }

    public void setValue(String key, String value) {
        attributes.put(key, value);
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    public String getExternalId() {
        return externalId;
    }

    public void setExternalId(String externalId) {
        this.externalId = externalId;
    }

    public long getRequestTimestamp() {
        return requestTimestamp;
    }

    public void setRequestTimestamp(long requestTimestamp) {
        this.requestTimestamp = requestTimestamp;
    }
}
