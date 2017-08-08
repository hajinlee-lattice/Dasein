package com.latticeengines.domain.exposed.metadata.statistics;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.statistics.Bucket;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class TopAttribute {

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

    public TopAttribute(String attribute, Long count) {
        this.attribute = attribute;
        this.count = count;
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
