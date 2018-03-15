package com.latticeengines.domain.exposed.cdl;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.FailingStep;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ProcessAnalyzeRequest {

    @JsonProperty("RebuildEntities")
    private Set<BusinessEntity> rebuildEntities = new HashSet<>();

    @JsonProperty("UserId")
    private String userId;

    @JsonProperty("FailingStep")
    private FailingStep failingStep;

    public Set<BusinessEntity> getRebuildEntities() {
        return rebuildEntities;
    }

    public void setRebuildEntities(Set<BusinessEntity> rebuildEntities) {
        this.rebuildEntities = rebuildEntities;
    }

    public String getUserId() {
        return userId;
    }

    public void setUserId(String userId) {
        this.userId = userId;
    }

    public FailingStep getFailingStep() {
        return failingStep;
    }

    public void setFailingStep(FailingStep failingStep) {
        this.failingStep = failingStep;
    }
}
