package com.latticeengines.domain.exposed.modelquality;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class AnalyticPipelineEntityNames {

    @JsonProperty("name")
    @ApiModelProperty(required = true)
    private String name;

    @JsonProperty("pipeline_name")
    @ApiModelProperty(required = true)
    private String pipeline;

    @JsonProperty("algorithm_name")
    @ApiModelProperty(required = true)
    private String algorithm;

    @JsonProperty("prop_data_name")
    @ApiModelProperty(required = true)
    private String propData;

    @JsonProperty("dataflow_name")
    @ApiModelProperty(required = true)
    private String dataFlow;

    @JsonProperty("sampling_name")
    @ApiModelProperty(required = true)
    private String sampling;

    @JsonIgnore
    private Integer version;

    public AnalyticPipelineEntityNames() {
    }

    public AnalyticPipelineEntityNames(AnalyticPipeline ap) {
        this.name = ap.getName();
        this.pipeline = ap.getPipeline().getName();
        this.algorithm = ap.getAlgorithm().getName();
        this.propData = ap.getPropData().getName();
        this.dataFlow = ap.getDataFlow().getName();
        this.sampling = ap.getSampling().getName();
        this.version = ap.getVersion();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getPipeline() {
        return pipeline;
    }

    public void setPipeline(String pipeline) {
        this.pipeline = pipeline;
    }

    public String getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(String algorithm) {
        this.algorithm = algorithm;
    }

    public String getPropData() {
        return propData;
    }

    public void setPropData(String propData) {
        this.propData = propData;
    }

    public String getDataFlow() {
        return dataFlow;
    }

    public void setDataFlow(String dataFlow) {
        this.dataFlow = dataFlow;
    }

    public String getSampling() {
        return sampling;
    }

    public void setSampling(String sampling) {
        this.sampling = sampling;
    }

    public Integer getVersion() {
        return version;
    }

    public void setVersion(Integer version) {
        this.version = version;
    }
}
