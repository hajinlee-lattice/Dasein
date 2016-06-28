package com.latticeengines.domain.exposed.modelquality;

import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * This class represents the holder for all the top-level dimensions. This class would be passed to the 
 * workflow that would kick off either the file-based self-service modeling or the event table model workflow.
 * 
 * @author rgonzalez
 *
 */
public class SelectedConfig {
    
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

    @JsonProperty("sampliing")
    private Sampling sampling;


    public Pipeline getPipeline() {
        return pipeline;
    }

    public void setPipeline(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public Algorithm getAlgorithm() {
        return algorithm;
    }

    public void setAlgorithm(Algorithm algorithm) {
        this.algorithm = algorithm;
    }

    public DataSet getDataSet() {
        return dataSet;
    }

    public void setDataSet(DataSet dataSet) {
        this.dataSet = dataSet;
    }

    public PropData getPropData() {
        return propData;
    }

    public void setPropData(PropData propData) {
        this.propData = propData;
    }

    public DataFlow getDataFlow() {
        return dataFlow;
    }

    public void setDataFlow(DataFlow dataFlow) {
        this.dataFlow = dataFlow;
    }

    public void setSampling(Sampling sampling) {
        this.sampling = sampling;
    }
}
