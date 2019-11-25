package com.latticeengines.domain.exposed.datacloud.transformation;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.transformation.step.TransformationStepConfig;

/**
 * Keep doc
 * https://confluence.lattice-engines.com/display/ENG/DataCloud+Engine+Architecture#DataCloudEngineArchitecture-Transformation
 * up to date if there is any new change
 */
@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
public class PipelineTransformationRequest {

    // Submitter of pipeline, eg. customer ID
    @JsonProperty("Submitter")
    private String submitter;

    // Unique pipeline name to identify a pipeline
    @JsonProperty("Name")
    private String name;

    // Target version of a pipeline job
    // All the target sources in the pipeline are generated with same target
    // version
    // If not provided, use current timestamp to create target version
    @JsonProperty("Version")
    private String version;

    // If true, temporary target sources (output of steps without target source
    // specified) generated in the pipeline will be retained if (and only if)
    // the pipeline job fails. When the failed pipeline is retried, finished
    // steps will be skipped (judged by whether their target sources exist)
    //
    // If false (default value), temporary target sources generated in the
    // pipeline will be deleted.
    @JsonProperty("KeepTemp")
    private boolean keepTemp;

    // List of pipeline step config
    @JsonProperty("Steps")
    private List<TransformationStepConfig> steps;

    // Whether to send notification in PropData Slack channel to track pipeline
    // job progress
    @JsonProperty("EnableSlack")
    private boolean enableSlack;

    // Memory setting for pipeline workflow container (NOT dataflow job)
    @JsonProperty("ContainerMemMB")
    private Integer containerMemMB;

    // Whether the pipeline is for AM rebuild
    // If true:
    // 1. Will set {@link
    // PipelineTransformationReportByStep#setDatacloudVersion(String)} with
    // version of AccountMaster being built
    // 2. If base source of specific base version doesn't exist on HDFS, will
    // attempt to copy the version from S3
    @JsonProperty("IsAMJob")
    private boolean isAMJob;

    public String getSubmitter() {
        return submitter;
    }

    public void setSubmitter(String creator) {
        this.submitter = creator;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getVersion() {
        return version;
    }

    public void setVersion(String version) {
        this.version = version;
    }

    public boolean getKeepTemp() {
        return keepTemp;
    }

    public void setKeepTemp(boolean keepTemp) {
        this.keepTemp = keepTemp;
    }

    public List<TransformationStepConfig> getSteps() {
        return steps;
    }

    public void setSteps(List<TransformationStepConfig> steps) {
        this.steps = steps;
    }

    public boolean isEnableSlack() {
        return enableSlack;
    }

    public void setEnableSlack(boolean enableSlack) {
        this.enableSlack = enableSlack;
    }

    public Integer getContainerMemMB() {
        return containerMemMB;
    }

    public void setContainerMemMB(Integer containerMemMB) {
        this.containerMemMB = containerMemMB;
    }

    public boolean isAMJob() {
        return isAMJob;
    }

    public void setAMJob(boolean isAMJob) {
        this.isAMJob = isAMJob;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }
}
