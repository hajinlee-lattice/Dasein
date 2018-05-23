package com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps;

import java.util.List;
import java.util.Set;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.serviceflows.core.steps.MicroserviceStepConfiguration;

public class AWSPythonBatchConfiguration extends MicroserviceStepConfiguration {

    @JsonProperty("is_run_in_aws")
    private boolean runInAws = true;

    @JsonProperty("inputPaths")
    private List<String> inputPaths;
    @JsonProperty("outputPath")
    private String outputPath;

    @JsonProperty("rebuild_steps")
    private Set<String> rebuildSteps;

    @JsonProperty("job_id")
    private String jobId;

    @JsonProperty("submission")
    private boolean submission;

    public boolean isRunInAws() {
        return runInAws;
    }

    public void setRunInAws(boolean runInAws) {
        this.runInAws = runInAws;
    }

    public void setInputPaths(List<String> inputPaths) {
        this.inputPaths = inputPaths;
    }

    public List<String> getInputPaths() {
        return inputPaths;
    }

    public void setOutputPath(String outputPath) {
        this.outputPath = outputPath;
    }

    public String getOutputPath() {
        return outputPath;
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

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    public boolean isSubmission() {
        return submission;
    }

    public void setSubmission(boolean submission) {
        this.submission = submission;
    }


}
