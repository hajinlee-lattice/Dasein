package com.latticeengines.domain.exposed.modelquality;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class ModelRunEntityNames {

    @JsonProperty("name")
    @ApiModelProperty(required = true)
    private String name;

    @JsonProperty("description")
    @ApiModelProperty(required = true)
    private String description;

    @JsonProperty("analytic_pipeline_name")
    @ApiModelProperty(required = true)
    private String analyticPipelineName;

    @JsonProperty("dataset_name")
    @ApiModelProperty(required = true)
    private String dataSetName;

    public ModelRunEntityNames() {
    }

    public ModelRunEntityNames(ModelRun modelrun) {
        this.name = modelrun.getName();
        this.description = modelrun.getDescription();
        this.analyticPipelineName = modelrun.getAnalyticPipeline().getName();
        this.dataSetName = modelrun.getDataSet().getName();
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public String getDescription() {
        return description;
    }

    public void setAnalyticPipelineName(String analyticPipelineName) {
        this.analyticPipelineName = analyticPipelineName;
    }

    public String getAnalyticPipelineName() {
        return analyticPipelineName;
    }

    public void setDataSetName(String dataSetName) {
        this.dataSetName = dataSetName;
    }

    public String getDataSetName() {
        return dataSetName;
    }
}
