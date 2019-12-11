package com.latticeengines.domain.exposed.serviceflows.cdl.steps.maintenance;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.workflow.BaseWrapperStepConfiguration;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "name")
@JsonSubTypes({
        @Type(value = SoftDeleteAccountConfiguration.class, name = "SoftDeleteAccountConfiguration"),
        @Type(value = SoftDeleteContactConfiguration.class, name = "SoftDeleteContactConfiguration"),
        @Type(value = SoftDeleteTransactionConfiguration.class, name = "SoftDeleteTransactionConfiguration")
})
public abstract class BaseSoftDeleteEntityConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("need_replace")
    private boolean needReplace;

    public abstract BusinessEntity getMainEntity();

    public boolean isNeedReplace() {
        return needReplace;
    }

    public void setNeedReplace(boolean needReplace) {
        this.needReplace = needReplace;
    }
}
