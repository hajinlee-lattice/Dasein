package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public abstract class BaseProcessEntityStepConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("rebuild")
    private Boolean rebuild;

    public abstract BusinessEntity getMainEntity();

    public Boolean getRebuild() {
        return rebuild;
    }

    public void setRebuild(Boolean rebuild) {
        this.rebuild = rebuild;
    }
}
