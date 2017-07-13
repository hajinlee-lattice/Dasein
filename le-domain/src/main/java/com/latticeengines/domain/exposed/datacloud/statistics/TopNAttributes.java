package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class TopNAttributes {

    @JsonProperty("SubCategories")
    private Map<String, List<TopAttribute>> topAttributes;

    @JsonProperty("EnrichmentAttributes")
    private List<LeadEnrichmentAttribute> enrichmentAttributes;

    public Map<String, List<TopAttribute>> getTopAttributes() {
        return topAttributes;
    }

    public void setTopAttributes(Map<String, List<TopAttribute>> topAttributes) {
        this.topAttributes = topAttributes;
    }

    public void addTopAttribute(String subCategory, TopAttribute attribute) {
        if (topAttributes == null) {
            topAttributes = new HashMap<>();
        }
        if (!topAttributes.containsKey(subCategory)) {
            topAttributes.put(subCategory, new ArrayList<>());
        }
        topAttributes.get(subCategory).add(attribute);
    }

    public List<LeadEnrichmentAttribute> getEnrichmentAttributes() {
        return enrichmentAttributes;
    }

    public void setEnrichmentAttributes(List<LeadEnrichmentAttribute> enrichmentAttributes) {
        this.enrichmentAttributes = enrichmentAttributes;
    }

    public static class TopAttribute {

        @JsonProperty("Attribute")
        private String attribute;

        @JsonProperty("NonNullCount")
        private Long nonNullCount;

        // dummy constructor for jackson
        @SuppressWarnings("unused")
        private TopAttribute() {
        }

        public TopAttribute(String attribute, Long nonNullCount) {
            this.attribute = attribute;
            this.nonNullCount = nonNullCount;
        }

        public String getAttribute() {
            return attribute;
        }

        public void setAttribute(String attribute) {
            this.attribute = attribute;
        }

        public Long getNonNullCount() {
            return nonNullCount;
        }

        public void setNonNullCount(Long nonNullCount) {
            this.nonNullCount = nonNullCount;
        }
    }
}
