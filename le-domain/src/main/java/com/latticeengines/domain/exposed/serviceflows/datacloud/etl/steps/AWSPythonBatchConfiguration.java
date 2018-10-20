package com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class AWSPythonBatchConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("is_run_in_aws")
    private boolean runInAws = true;

    @JsonProperty("inputPaths")
    private List<String> inputPaths;

    @JsonProperty("outputPath")
    private String outputPath;

    @JsonProperty("tableName")
    private String tableName;

    @JsonProperty("rebuild_steps")
    private Set<String> rebuildSteps;

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("submission")
    private boolean submission;

    @JsonProperty("version")
    private Version version;

    @JsonProperty("rolling_period")
    private String rollingPeriod;

    public boolean isRunInAws() {
        return runInAws;
    }

    public void setRunInAws(boolean runInAws) {
        this.runInAws = runInAws;
    }

    public List<String> getInputPaths() {
        return inputPaths;
    }

    public void setInputPaths(List<String> inputPaths) {
        this.inputPaths = inputPaths;
    }

    public String getOutputPath() {
        return outputPath;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public Set<String> getRebuildSteps() {
        return rebuildSteps;
    }

    public void setRebuildSteps(Set<String> rebuildSteps) {
        this.rebuildSteps = rebuildSteps;
    }

    public String getJobId() {
        return jobId;
    }

    public void setJobId(String jobId) {
        this.jobId = jobId;
    }

    public boolean isSubmission() {
        return submission;
    }

    public void setSubmission(boolean submission) {
        this.submission = submission;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public Version getVersion() {
        return version;
    }

    public void setVersion(Version version) {
        this.version = version;
    }

    public String getRollingPeriod() {
        return rollingPeriod;
    }

    public void setRollingPeriod(String rollingPeriod) {
        this.rollingPeriod = rollingPeriod;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
