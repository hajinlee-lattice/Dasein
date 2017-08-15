package com.latticeengines.domain.exposed.metadata.statistics;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TopAttribute {

    @JsonProperty("Entity")
    private BusinessEntity entity;

    @JsonProperty("Attribute")
    private String attribute;

    @JsonProperty("Count")
    private Long count;

    @JsonProperty("TopBkt")
    private Bucket topBkt;

    // dummy constructor for jackson
    @SuppressWarnings("unused")
    private TopAttribute() {
    }

    public TopAttribute(AttributeLookup attributeLookup, Long count) {
        this.entity = attributeLookup.getEntity();
        this.attribute = attributeLookup.getAttribute();
        this.count = count;
    }

    public BusinessEntity getEntity() {
        return entity;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }

    public Long getCount() {
        return count;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public Bucket getTopBkt() {
        return topBkt;
    }

    public void setTopBkt(Bucket topBkt) {
        this.topBkt = topBkt;
    }
}
