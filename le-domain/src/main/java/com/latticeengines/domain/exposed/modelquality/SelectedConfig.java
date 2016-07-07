package com.latticeengines.domain.exposed.modelquality;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricTagGroup;

/**
 * This class represents the holder for all the top-level dimensions. This class
 * would be passed to the workflow that would kick off either the file-based
 * self-service modeling or the event table model workflow.
 * 
 * @author rgonzalez
 * 
 */
public class SelectedConfig implements Fact, Dimension {

    @JsonProperty("pipeline")
    private Pipeline pipeline;

    @JsonProperty("algorithm")
    private Algorithm algorithm;

    @JsonProperty("data_set")
    private DataSet dataSet;

    @JsonProperty("prop_data")
    private PropData propData;

    @JsonProperty("data_flow")
    private DataFlow dataFlow;

    @JsonProperty("sampling")
    private Sampling sampling;

    @MetricTagGroup
    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    @MetricTagGroup
    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    @MetricTagGroup
    public DataSet getDataSet() {
        return dataSet;
    }

    public void setDataSet(DataSet dataSet) {
        this.dataSet = dataSet;
    }


    @MetricTagGroup
    public PropData getPropData() {
        return propData;
    }

    public void setPropData(PropData propData) {
        this.propData = propData;
    }

    @MetricTagGroup
    public DataFlow getDataFlow() {
        return dataFlow;
    }

    public void setDataFlow(DataFlow dataFlow) {
        this.dataFlow = dataFlow;
    }

    @MetricTagGroup
    public Sampling getSampling() {
        return sampling;
    }

    public void setSampling(Sampling sampling) {
        this.sampling = sampling;
    }
}
