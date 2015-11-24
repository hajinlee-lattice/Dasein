package com.latticeengines.serviceflows.workflow.dataflow;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.validator.annotation.NotEmptyString;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.serviceflows.workflow.core.MicroserviceStepConfiguration;

public class DataFlowStepConfiguration extends MicroserviceStepConfiguration {

    @NotEmptyString
    @NotNull
    private String flowName;

    @NotEmptyString
    @NotNull
    private String dataflowBeanName;

    @NotEmptyString
    @NotNull
    private String targetPath;

    @NotEmptyString
    @NotNull
    private String stoplistAvroFile;

    @NotEmptyString
    @NotNull
    private String stoplistPath;

    @JsonProperty("flow_name")
    public String getFlowName() {
        return flowName;
    }

    @JsonProperty("flow_name")
    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    @JsonProperty("bean_name")
    public String getDataflowBeanName() {
        return dataflowBeanName;
    }

    @JsonProperty("bean_name")
    public void setDataflowBeanName(String dataflowBeanName) {
        this.dataflowBeanName = dataflowBeanName;
    }

    @JsonProperty("target_path")
    public String getTargetPath() {
        return targetPath;
    }

    @JsonProperty("target_path")
    public void setTargetPath(String targetPath) {
        this.targetPath = targetPath;
    }

    @JsonProperty("stoplist_avrofile")
    public String getStoplistAvroFile() {
        return stoplistAvroFile;
    }

    @JsonProperty("stoplist_avrofile")
    public void setStoplistAvroFile(String stoplistAvroFile) {
        this.stoplistAvroFile = stoplistAvroFile;
    }

    @JsonProperty("stoplist_path")
    public String getStoplistPath() {
        return stoplistPath;
    }

    @JsonProperty("stoplist_path")
    public void setStoplistPath(String stoplistPath) {
        this.stoplistPath = stoplistPath;
    }

}
