package com.latticeengines.aws.batch;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;

public class AWSBatchConfig {

    @JsonProperty("inputPaths")
    private List<String> inputPaths;

    @JsonProperty("outputPath")
    private String outputPath;

    @JsonProperty("job_id")
    private String jobId;

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

}
