package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.datacloud.match.entity.EntityMatchConfiguration;
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
        @Type(value = BuildCatalogStepConfiguration.class, name = "BuildCatalogStepConfiguration"),
        @Type(value = ProcessActivityStreamStepConfiguration.class, name = "ProcessActivityStreamStepConfiguration"),
        @Type(value = CuratedAccountAttributesStepConfiguration.class, name = "CuratedAccountAttributesStepConfiguration"),
        @Type(value = ProcessRatingStepConfiguration.class, name = "ProcessRatingStepConfiguration"),
        @Type(value = ProfileAccountActivityMetricsStepConfiguration.class, name = "ProfileAccountActivityMetricsStepConfiguration"),
        @Type(value = ProfileContactActivityMetricsStepConfiguration.class, name = "ProfileContactActivityMetricsStepConfiguration") })
public abstract class BaseProcessEntityStepConfiguration extends BaseWrapperStepConfiguration {

    @JsonProperty("rebuild")
    private Boolean rebuild;

    @JsonProperty("need_replace")
    private boolean needReplace;

    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;

    @JsonProperty("erase_by_null_enabled")
    private boolean eraseByNullEnabled;

    @JsonProperty("system_ids")
    private Map<String, List<String>> systemIdMap; // entity -> List<ID> used for matching

    @JsonProperty("default_system_ids")
    private Map<String, String> defaultSystemIdMap; // entity -> ID that mapped to lattice ID

    @JsonProperty("entity_match_configuration")
    private EntityMatchConfiguration entityMatchConfiguration;

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

    public boolean getNeedReplace() {
        return needReplace;
    }

    public void setNeedReplace(boolean needReplace) {
        this.needReplace = needReplace;
    }

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }

    public EntityMatchConfiguration getEntityMatchConfiguration() {
        return entityMatchConfiguration;
    }

    public void setEntityMatchConfiguration(EntityMatchConfiguration entityMatchConfiguration) {
        this.entityMatchConfiguration = entityMatchConfiguration;
    }

    public boolean isEraseByNullEnabled() {
        return eraseByNullEnabled;
    }

    public void setEraseByNullEnabled(boolean eraseByNullEnabled) {
        this.eraseByNullEnabled = eraseByNullEnabled;
    }
}
