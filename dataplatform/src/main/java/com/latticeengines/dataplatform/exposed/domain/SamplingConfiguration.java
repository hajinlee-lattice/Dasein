package com.latticeengines.dataplatform.exposed.domain;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.util.JsonHelper;

public class SamplingConfiguration {

    private List<SamplingElement> samplingElements = new ArrayList<SamplingElement>();
    private int trainingPercentage;
    private int testPercentage;
    
    public void addSamplingElement(SamplingElement samplingElement) {
        samplingElements.add(samplingElement);
    }

    @JsonProperty("sampling_elements")
    public List<SamplingElement> getSamplingElements() {
        return samplingElements;
    }

    @JsonProperty("sampling_elements")
    public void setSamplingElements(List<SamplingElement> samplingElements) {
        this.samplingElements = samplingElements;
    }
    
    @Override
    public String toString() {
        return JsonHelper.serialize(this);
    }

    @JsonProperty("training_percentage")
    public int getTrainingPercentage() {
        return trainingPercentage;
    }

    @JsonProperty("training_percentage")
    public void setTrainingPercentage(int trainingPercentage) {
        assert(trainingPercentage <= 100);
        this.trainingPercentage = trainingPercentage;
        setTestPercentage(100 - trainingPercentage);
    }

    @JsonProperty("test_percentage")
    public int getTestPercentage() {
        return testPercentage;
    }

    @JsonProperty("test_percentage")
    public void setTestPercentage(int testPercentage) {
        assert(testPercentage <= 100);
        this.testPercentage = testPercentage;
        
    }
}
