package com.latticeengines.domain.exposed.cdl;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * this configuration is used to run loadTesting.
 *
 * checkpointName & checkpointVersion: this is we used to saveCheckpoint, if you won't save checkpoint, can set null
 * resumeCheckpointName & resumeCheckpointVersion: identify which checkpoint you want to resume from.
 * tips: if you set checkpointName/resumeCheckpointName, haven't set version, will use the default version.
 *
 * isAuto: if true, will save all systemInfo(about s3ImportSystem, datafeedTask, TemplateTable, AtlasStream, Catalog,
 * metricGroup, dimensionMetadata.
 *
 * runPA:if true, will run PA after all import finished.
 * processAnalyzeRequest: will use this request to run PA.
 *
 * imports: used in MockCSVImport() method to import data.
 *
 * featureFlagMap: use to create test tenant.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public class End2EndTestConfiguration {

    @JsonProperty("checkpoint_name")
    private String checkpointName;

    @JsonProperty("checkpoint_version")
    private String checkpointVersion;

    @JsonProperty("resume_checkpoint_name")
    private String resumeCheckpointName;

    @JsonProperty("resume_checkpoint_version")
    private String resumeCheckpointVersion;

    @JsonProperty("is_auto")
    private boolean isAuto;

    @JsonProperty("run_pa")
    private boolean runPA;

    @JsonProperty("process_analyze_request")
    private ProcessAnalyzeRequest processAnalyzeRequest;

    @JsonProperty("imports")
    private List<MockImport> imports;

    @JsonProperty("feature_flag_map")
    private Map<String, Boolean> featureFlagMap;

    public String getCheckpointName() {
        return checkpointName;
    }

    public void setCheckpointName(String checkpointName) {
        this.checkpointName = checkpointName;
    }

    public String getCheckpointVersion() {
        return checkpointVersion;
    }

    public void setCheckpointVersion(String checkpointVersion) {
        this.checkpointVersion = checkpointVersion;
    }

    public boolean isAuto() {
        return isAuto;
    }

    public void setAuto(boolean auto) {
        isAuto = auto;
    }

    public List<MockImport> getImports() {
        return imports;
    }

    public void setImports(List<MockImport> imports) {
        this.imports = imports;
    }

    public Map<String, Boolean> getFeatureFlagMap() {
        return featureFlagMap;
    }

    public void setFeatureFlagMap(Map<String, Boolean> featureFlagMap) {
        this.featureFlagMap = featureFlagMap;
    }

    public String getResumeCheckpointName() {
        return resumeCheckpointName;
    }

    public void setResumeCheckpointName(String resumeCheckpointName) {
        this.resumeCheckpointName = resumeCheckpointName;
    }

    public String getResumeCheckpointVersion() {
        return resumeCheckpointVersion;
    }

    public void setResumeCheckpointVersion(String resumeCheckpointVersion) {
        this.resumeCheckpointVersion = resumeCheckpointVersion;
    }

    public boolean isRunPA() {
        return runPA;
    }

    public void setRunPA(boolean runPA) {
        this.runPA = runPA;
    }

    public ProcessAnalyzeRequest getProcessAnalyzeRequest() {
        return processAnalyzeRequest;
    }

    public void setProcessAnalyzeRequest(ProcessAnalyzeRequest processAnalyzeRequest) {
        this.processAnalyzeRequest = processAnalyzeRequest;
    }
}
