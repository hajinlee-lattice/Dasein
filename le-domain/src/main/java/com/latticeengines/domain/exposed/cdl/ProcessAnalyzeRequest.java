package com.latticeengines.domain.exposed.cdl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ProcessAnalyzeRequest {

    @JsonProperty("RebuildEntities")
    private List<BusinessEntity> rebuildEntities;

    public List<BusinessEntity> getRebuildEntities() {
        return rebuildEntities;
    }

    public void setRebuildEntities(List<BusinessEntity> rebuildEntities) {
        this.rebuildEntities = rebuildEntities;
    }

}
