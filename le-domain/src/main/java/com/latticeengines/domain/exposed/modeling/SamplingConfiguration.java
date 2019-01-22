package com.latticeengines.domain.exposed.modeling;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

public class SamplingConfiguration {

    public static final String TRAINING_SET_PREFIX = "TrainingSet";
    public static final String TRAINING_ALL_PREFIX = "allTraining";
    public static final String TESTING_SET_PREFIX = "allTest";
    private static final SamplingType DEFAULT_SAMPLING_TYPE = SamplingType.DEFAULT_SAMPLING;

    private List<SamplingElement> samplingElements = new ArrayList<SamplingElement>();
    @NotNull
    private String customer;

    /* parallel */
    @NotNull
    private String table;
    private int trainingPercentage = 80;
    private int testPercentage = 20;
    private int samplingRate = 100;
    private int trainingSetCount = 1;
    private Map<String, String> properties = new HashMap<String, String>();
    private List<SamplingElement> trainingElements = new ArrayList<SamplingElement>();
    private SamplingElement trainingAll = new SamplingElement(TRAINING_ALL_PREFIX);
    private SamplingElement testingElement = new SamplingElement(TESTING_SET_PREFIX);

    private boolean parallelEnabled;
    private String hdfsDirPath;
    private Long randomSeed = -1L;
    private SamplingType samplingType = SamplingType.STRATIFIED_SAMPLING;
    private Map<String, Long> counterGroupResultMap;

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
        return JsonUtils.serialize(this);
    }

    @JsonProperty("training_percentage")
    public int getTrainingPercentage() {
        return trainingPercentage;
    }

    @JsonProperty("training_percentage")
    public void setTrainingPercentage(int trainingPercentage) {
        checkNumberRange(trainingPercentage);
        this.trainingPercentage = trainingPercentage;
        setTestPercentage(100 - trainingPercentage);
    }

    @JsonProperty("test_percentage")
    public int getTestPercentage() {
        return testPercentage;
    }

    @JsonProperty("test_percentage")
    public void setTestPercentage(int testPercentage) {
        assert (testPercentage <= 100);
        this.testPercentage = testPercentage;

    }

    @JsonProperty("customer")
    public String getCustomer() {
        return customer;
    }

    @JsonProperty("customer")
    public void setCustomer(String customer) {
        this.customer = customer;
    }

    @JsonProperty("table")
    public String getTable() {
        return table;
    }

    @JsonProperty("table")
    public void setTable(String table) {
        this.table = table;
    }

    /* parallel */

    public void setTrainingElements() {
        trainingElements.clear();
        for (int i = 0; i < trainingSetCount; i++) {
            SamplingElement element = new SamplingElement(TRAINING_SET_PREFIX + i);
            trainingElements.add(element);
        }
        samplingElements.clear();
        createDefaultSamplingElements();
        samplingElements.addAll(trainingElements);
    }

    @JsonProperty("training_elements")
    public List<SamplingElement> getTrainingElements() {
        return trainingElements;
    }

    @JsonProperty("testing_element")
    public SamplingElement getTestingElement() {
        return testingElement;
    }

    @JsonProperty("property")
    @JsonAnySetter
    public void setProperty(String propertyName, String propertyValue) {
        properties.put(propertyName, propertyValue);
    }

    @JsonProperty("property")
    public String getProperty(String propertyName) {
        return properties.get(propertyName);
    }

    @JsonProperty("property")
    @JsonAnyGetter
    public Map<String, String> getProperties() {
        return properties;
    }

    @JsonProperty("sampling_rate")
    public int getSamplingRate() {
        return samplingRate;
    }

    @JsonProperty("sampling_rate")
    public void setSamplingRate(int samplingRate) {
        checkNumberRange(samplingRate);
        this.samplingRate = samplingRate;
    }

    @JsonProperty("training_set_count")
    public int getTrainingSetCount() {
        return trainingSetCount;
    }

    @JsonProperty("training_set_count")
    public void setTrainingSetCount(int trainingSetCount) {
        this.trainingSetCount = trainingSetCount;
    }

    private void checkNumberRange(int number) {
        if (number > 100 || number <= 0) {
            throw new LedpException(LedpCode.LEDP_15012);
        }
    }

    @JsonProperty("sampling_type")
    public SamplingType getSamplingType() {
        // if(samplingType == null){
        //
        // }
        return samplingType;
    }

    @JsonProperty("sampling_type")
    public void setSamplingType(SamplingType samplingType) {
        this.samplingType = samplingType;
    }

    public void createDefaultSamplingElements() {
        samplingElements.add(trainingAll);
        samplingElements.add(testingElement);
    }

    @JsonProperty("parallel_enabled")
    public boolean isParallelEnabled() {
        return parallelEnabled;
    }

    @JsonProperty("parallel_enabled")
    public void setParallelEnabled(boolean parallelEnabled) {
        this.parallelEnabled = parallelEnabled;
    }

    @JsonProperty(value = "hdfs_dir_path", required = false)
    public String getHdfsDirPath() {
        return hdfsDirPath;
    }

    @JsonProperty(value = "hdfs_dir_path", required = false)
    public void setHdfsDirPath(String hdfsDirPath) {
        this.hdfsDirPath = hdfsDirPath;
    }

    @JsonProperty(value = "random_seed", required = false)
    public Long getRandomSeed() {
        return randomSeed;
    }

    @JsonProperty(value = "random_seed", required = false)
    public void setRandomSeed(Long randomSeed) {
        this.randomSeed = randomSeed;
    }

    @JsonProperty(value = "event_count_map", required = false)
    public Map<String, Long> getCounterGroupResultMap() {
        return counterGroupResultMap;
    }

    @JsonProperty(value = "event_count_map", required = false)
    public void setCounterGroupResultMap(Map<String, Long> counterGroupResultMap) {
        this.counterGroupResultMap = counterGroupResultMap;
    }

}
