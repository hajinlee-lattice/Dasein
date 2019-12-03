package com.latticeengines.domain.exposed.cdl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class UpdateSegmentCountResponse {

    @JsonProperty("UpdatedCounts")
    private Map<String, Map<BusinessEntity, Long>> updatedCounts;

    @JsonProperty("FailedSegments")
    private List<String> failedSegments;

    public Map<String, Map<BusinessEntity, Long>> getUpdatedCounts() {
        return updatedCounts;
    }

    public void setUpdatedCounts(Map<String, Map<BusinessEntity, Long>> updatedCounts) {
        this.updatedCounts = updatedCounts;
    }

    public List<String> getFailedSegments() {
        return failedSegments;
    }

    public void setFailedSegments(List<String> failedSegments) {
        this.failedSegments = failedSegments;
    }
}
