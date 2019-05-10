package com.latticeengines.domain.exposed.cdl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

    @JsonProperty("RebuildSteps")
    private List<String> rebuildSteps = new ArrayList<>();

    @JsonProperty("SkipEntities")
    private Set<BusinessEntity> skipEntities = new HashSet<>();

    @JsonProperty("IgnoreDataCloudChange")
    private Boolean ignoreDataCloudChange;

    @JsonProperty("UserId")
    private String userId;

    @JsonProperty("FailingStep")
    private FailingStep failingStep;

    @JsonProperty("MaxRatingIteration")
    private Integer MaxRatingIterations;

    // refresh all the results related to fuzzy match
    @JsonProperty("FullRematch")
    private Boolean fullRematch;

    // flag to force start another PA disregarding the number of currently running
    // PA in the cluster
    @JsonProperty("ForceRun")
    private Boolean forceRun;

    // flag to indiate if it's auto-scheduled
    @JsonProperty("AutoSchedule")
    private Boolean autoSchedule;

    /*
     * flag to inherit all import actions from last failed PA
     */
    @JsonProperty("InheritAllCompleteImportActions")
    private boolean inheritAllCompleteImportActions;

    /*
     * list of import action PIDs to inherit from last failed PA
     */
    @JsonProperty("ImportActionPidsToInherit")
    private List<Long> importActionPidsToInherit;

    /**
     * Skip publish to S3, mainly to speed up e2e on local
     */
    @JsonProperty("SkipPublishToS3")
    private Boolean skipPublishToS3;

    /**
     * Skip Export to Dynamo step. Mainly for QA
     */
    @JsonProperty("SkipDynamoExport")
    private Boolean skipDynamoExport;

    public Set<BusinessEntity> getRebuildEntities() {
        return rebuildEntities;
    }

    public void setRebuildEntities(Set<BusinessEntity> rebuildEntities) {
        this.rebuildEntities = rebuildEntities;
    }

    public List<String> getRebuildSteps() {
        return rebuildSteps;
    }

    public void setRebuildSteps(List<String> rebuildSteps) {
        this.rebuildSteps = rebuildSteps;
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

    public Boolean getIgnoreDataCloudChange() {
        return ignoreDataCloudChange;
    }

    public void setIgnoreDataCloudChange(Boolean ignoreDataCloudChange) {
        this.ignoreDataCloudChange = ignoreDataCloudChange;
    }

    public Integer getMaxRatingIterations() {
        return MaxRatingIterations;
    }

    public void setMaxRatingIterations(Integer maxRatingIterations) {
        MaxRatingIterations = maxRatingIterations;
    }

    public Set<BusinessEntity> getSkipEntities() {
        return skipEntities;
    }

    public void setSkipEntities(Set<BusinessEntity> skipEntities) {
        this.skipEntities = skipEntities;
    }

    public boolean isInheritAllCompleteImportActions() {
        return inheritAllCompleteImportActions;
    }

    public void setInheritAllCompleteImportActions(boolean inheritAllCompleteImportActions) {
        this.inheritAllCompleteImportActions = inheritAllCompleteImportActions;
    }

    public List<Long> getImportActionPidsToInherit() {
        return importActionPidsToInherit;
    }

    public void setImportActionPidsToInherit(List<Long> importActionPidsToInherit) {
        this.importActionPidsToInherit = importActionPidsToInherit;
    }

    public Boolean getForceRun() {
        return forceRun;
    }

    public void setForceRun(Boolean forceRun) {
        this.forceRun = forceRun;
    }

    public Boolean getAutoSchedule() {
        return autoSchedule;
    }

    public void setAutoSchedule(Boolean autoSchedule) {
        this.autoSchedule = autoSchedule;
    }

    public Boolean getFullRematch() {
        return fullRematch;
    }

    public void setFullRematch(Boolean fullRematch) {
        this.fullRematch = fullRematch;
    }

    public Boolean getSkipPublishToS3() {
        return skipPublishToS3;
    }

    public void setSkipPublishToS3(Boolean skipPublishToS3) {
        this.skipPublishToS3 = skipPublishToS3;
    }

    public Boolean getSkipDynamoExport() {
        return skipDynamoExport;
    }

    public void setSkipDynamoExport(Boolean skipDynamoExport) {
        this.skipDynamoExport = skipDynamoExport;
    }
}
