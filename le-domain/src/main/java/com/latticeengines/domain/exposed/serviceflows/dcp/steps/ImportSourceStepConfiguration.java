package com.latticeengines.domain.exposed.serviceflows.dcp.steps;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusAppendConfig;
import com.latticeengines.domain.exposed.datacloud.match.config.DplusMatchConfig;
import com.latticeengines.domain.exposed.dcp.PurposeOfUse;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class ImportSourceStepConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("project_id")
    private String projectId;

    @JsonProperty("source_id")
    private String sourceId;

    @JsonProperty("upload_id")
    private String uploadId;

    @JsonProperty("stats_pid")
    private Long statsPid;

    @JsonProperty("match_config")
    private DplusMatchConfig matchConfig;

    @JsonProperty("append_config")
    private DplusAppendConfig appendConfig;

    @JsonProperty("match_purpose")
    private PurposeOfUse matchPurpose;

    @JsonProperty("append_purpose")
    private PurposeOfUse appendPurpose;

    @JsonProperty("suppress_known_errors")
    private Boolean suppressErrors;

    public String getProjectId() {
        return projectId;
    }

    public void setProjectId(String projectId) {
        this.projectId = projectId;
    }

    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getUploadId() {
        return uploadId;
    }

    public void setUploadId(String uploadId) {
        this.uploadId = uploadId;
    }

    public Long getStatsPid() {
        return statsPid;
    }

    public void setStatsPid(Long statsPid) {
        this.statsPid = statsPid;
    }

    public DplusMatchConfig getMatchConfig() {
        return matchConfig;
    }

    public void setMatchConfig(DplusMatchConfig matchConfig) {
        this.matchConfig = matchConfig;
    }

    public PurposeOfUse getMatchPurpose() {
        return matchPurpose;
    }

    public void setMatchPurpose(PurposeOfUse matchPurpose) {
        this.matchPurpose = matchPurpose;
    }

    public DplusAppendConfig getAppendConfig() {
        return appendConfig;
    }

    public void setAppendConfig(DplusAppendConfig appendConfig) {
        this.appendConfig = appendConfig;
    }

    public PurposeOfUse getAppendPurpose() {
        return appendPurpose;
    }

    public void setAppendPurpose(PurposeOfUse appendPurpose) {
        this.appendPurpose = appendPurpose;
    }

    public Boolean getSuppressErrors() {
        return suppressErrors;
    }

    public void setSuppressErrors(Boolean suppressErrors) {
        this.suppressErrors = suppressErrors;
    }
}
