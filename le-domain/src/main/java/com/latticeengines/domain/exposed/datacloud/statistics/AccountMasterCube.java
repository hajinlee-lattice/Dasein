package com.latticeengines.domain.exposed.datacloud.statistics;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class AccountMasterCube {
    @JsonProperty("Stats")
    private Map<String, AMAttributeStats> statistics;

    @JsonProperty("Cnt")
    private Long nonNullCount;

    @JsonProperty("EnrichmentAttributes")
    private List<LeadEnrichmentAttribute> enrichmentAttributes;

    public Map<String, AMAttributeStats> getStatistics() {
        return statistics;
    }

    public void setStatistics(Map<String, AMAttributeStats> statistics) {
        this.statistics = statistics;
    }

    public Long getNonNullCount() {
        return nonNullCount;
    }

    public void setNonNullCount(Long nonNullCount) {
        this.nonNullCount = nonNullCount;
    }

    public List<LeadEnrichmentAttribute> getEnrichmentAttributes() {
        return enrichmentAttributes;
    }

    public void setEnrichmentAttributes(List<LeadEnrichmentAttribute> enrichmentAttributes) {
        this.enrichmentAttributes = enrichmentAttributes;
    }
}
