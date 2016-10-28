package com.latticeengines.domain.exposed.modelquality;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

import io.swagger.annotations.ApiModelProperty;

public class AnalyticTestEntityNames {

    @JsonProperty("name")
    @ApiModelProperty(required = true)
    private String name;

    @JsonProperty("dataset_names")
    @ApiModelProperty(required = true)
    private List<String> dataSetNames = new ArrayList<>();

    @JsonProperty("match_type")
    @ApiModelProperty(required = true)
    private PropDataMatchType propDataMatchType;

    @JsonProperty("analytic_test_type")
    @ApiModelProperty(required = true)
    private AnalyticTestType analyticTestType;
    
    @JsonProperty("analytic_pipeline_names")
    @ApiModelProperty(required = true)
    private List<String> analyticPipelineNames = new ArrayList<>();

    public AnalyticTestEntityNames() {
    }

    public AnalyticTestEntityNames(AnalyticTest atest) {
        name = atest.getName();
        propDataMatchType = atest.getPropDataMatchType();
        analyticTestType = atest.getAnalyticTestType();
        for (DataSet dataset : atest.getDataSets()) {
            this.dataSetNames.add(dataset.getName());
        }
        for (AnalyticPipeline analyticPipeline : atest.getAnalyticPipelines()){
            this.analyticPipelineNames.add(analyticPipeline.getName());
        }
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public void setPropDataMatchType(PropDataMatchType propDataMatchType) {
        this.propDataMatchType = propDataMatchType;
    }

    public PropDataMatchType getPropDataMatchType() {
        return propDataMatchType;
    }
    
    public void setAnalyticTestType(AnalyticTestType analyticTestType) {
        this.analyticTestType = analyticTestType;
    }

    public AnalyticTestType getAnalyticTestType() {
        return analyticTestType;
    }

    public void setAnalyticPipelineNames(List<String> analyticPipelineNames) {
        this.analyticPipelineNames = analyticPipelineNames;
    }

    public List<String> getAnalyticPipelineNames() {
        return analyticPipelineNames;
    }

    public void setDataSetNames(List<String> dataSetNames) {
        this.dataSetNames = dataSetNames;
    }

    public List<String> getDataSetNames() {
        return dataSetNames;
    }
}
