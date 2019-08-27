package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;
import java.util.Map;

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
        @Type(value = ProcessAccountStepConfiguration.class, name = "ProcessAccountStepConfiguration"),
        @Type(value = ProcessContactStepConfiguration.class, name = "ProcessContactStepConfiguration"),
        @Type(value = ProcessProductStepConfiguration.class, name = "ProcessProductStepConfiguration"),
        @Type(value = ProcessTransactionStepConfiguration.class, name = "ProcessTransactionStepConfiguration"),
        @Type(value = CuratedAccountAttributesStepConfiguration.class, name = "CuratedAccountAttributesStepConfiguration"),
        @Type(value = ProcessRatingStepConfiguration.class, name = "ProcessRatingStepConfiguration"), })
public abstract class BaseProcessEntityStepConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("rebuild")
    private Boolean rebuild;

    @JsonProperty("need_cleanup")
    private Boolean needCleanup;

    @JsonProperty("system_ids")
    private Map<String, List<String>> systemIdMap; // entity -> List<ID> used for matching

    @JsonProperty("default_system_ids")
    private Map<String, String> defaultSystemIdMap; // entity -> ID that mapped to lattice ID

    public abstract BusinessEntity getMainEntity();

    public Boolean getRebuild() {
        return rebuild;
    }

    public void setRebuild(Boolean rebuild) {
        this.rebuild = rebuild;
    }

    public Map<String, List<String>> getSystemIdMap() {
        return systemIdMap;
    }

    public void setSystemIdMap(Map<String, List<String>> systemIdMap) {
        this.systemIdMap = systemIdMap;
    }

    public Map<String, String> getDefaultSystemIdMap() {
        return defaultSystemIdMap;
    }

    public void setDefaultSystemIdMap(Map<String, String> defaultSystemIdMap) {
        this.defaultSystemIdMap = defaultSystemIdMap;
    }

    public Boolean getNeedCleanup() {
        return needCleanup;
    }

    public void setNeedCleanup(Boolean needCleanup) {
        this.needCleanup = needCleanup;
    }
}
