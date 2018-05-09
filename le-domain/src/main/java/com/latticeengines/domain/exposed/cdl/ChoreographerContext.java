package com.latticeengines.domain.exposed.cdl;

import java.util.HashSet;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ChoreographerContext {

    @JsonProperty
    private Set<BusinessEntity> jobImpactedEntities = new HashSet<>();

    @JsonProperty
    private boolean dataCloudChanged;

    public Set<BusinessEntity> getJobImpactedEntities() {
        return jobImpactedEntities;
    }

    public void setJobImpactedEntities(Set<BusinessEntity> jobImpactedEntities) {
        this.jobImpactedEntities = jobImpactedEntities;
    }

    public boolean isDataCloudChanged() {
        return dataCloudChanged;
    }

    public void setDataCloudChanged(boolean dataCloudChanged) {
        this.dataCloudChanged = dataCloudChanged;
    }

}
