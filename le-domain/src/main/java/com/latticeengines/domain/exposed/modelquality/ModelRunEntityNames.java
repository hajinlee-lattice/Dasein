package com.latticeengines.domain.exposed.modelquality;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

import io.swagger.annotations.ApiModelProperty;

public class ModelRunEntityNames implements Dimension {

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

    @JsonProperty("analytic_test_name")
    @ApiModelProperty(required = false)
    private String analyticTestName;

    @JsonProperty("analytic_test_tag")
    @ApiModelProperty(required = false)
    private String analyticTestTag;

    public ModelRunEntityNames() {
    }

    public ModelRunEntityNames(ModelRun modelrun) {
        this.name = modelrun.getName();
        this.description = modelrun.getDescription();
        this.analyticPipelineName = modelrun.getAnalyticPipeline().getName();
        this.dataSetName = modelrun.getDataSet().getName();
        this.analyticTestName = modelrun.getAnalyticTestName();
        this.analyticTestTag = modelrun.getAnalyticTestTag();
    }

    public void setName(String name) {
        this.name = name;
    }

    @MetricTag(tag = "ModelName")
    public String getName() {
        return name;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    @MetricTag(tag = "ModelDescription")
    public String getDescription() {
        return description;
    }

    public void setAnalyticPipelineName(String analyticPipelineName) {
        this.analyticPipelineName = analyticPipelineName;
    }

    @MetricTag(tag = "AnalyticPipelineName")
    public String getAnalyticPipelineName() {
        return analyticPipelineName;
    }

    public void setDataSetName(String dataSetName) {
        this.dataSetName = dataSetName;
    }

    @MetricTag(tag = "DatasetName")
    public String getDataSetName() {
        return dataSetName;
    }

    public void setAnalyticTestName(String analyticTestName) {
        this.analyticTestName = analyticTestName;
    }

    @MetricTag(tag = "AnalyticTestName")
    public String getAnalyticTestName() {
        return analyticTestName;
    }

    public void setAnalyticTestTag(String analyticTestTag) {
        this.analyticTestTag = analyticTestTag;
    }

    @MetricTag(tag = "AnalyticTestTag")
    public String getAnalyticTestTag() {
        return analyticTestTag;
    }
}
