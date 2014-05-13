package com.latticeengines.dataplatform.exposed.domain;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.common.exposed.util.JsonUtils;

public class Classifier implements HasName {

    private String name;
    private String trainingDataHdfsPath;
    private String testDataHdfsPath;
    private String pythonScriptHdfsPath;
    private String modelHdfsDir;
    private String schemaHdfsPath;
    private List<String> keyCols = new ArrayList<String>();
    private List<String> features = new ArrayList<String>();
    private List<String> targets = new ArrayList<String>();
    private String dataFormat;
    private String algorithmProperties;

    @Override
    @JsonProperty("name")
    public String getName() {
        return name;
    }

    @Override
    @JsonProperty("name")
    public void setName(String name) {
        this.name = name;
    }

    @JsonProperty("training_data")
    public String getTrainingDataHdfsPath() {
        return trainingDataHdfsPath;
    }

    @JsonProperty("training_data")
    public void setTrainingDataHdfsPath(String trainingDataHdfsPath) {
        this.trainingDataHdfsPath = trainingDataHdfsPath;
    }

    @JsonProperty("test_data")
    public String getTestDataHdfsPath() {
        return testDataHdfsPath;
    }

    @JsonProperty("test_data")
    public void setTestDataHdfsPath(String testDataHdfsPath) {
        this.testDataHdfsPath = testDataHdfsPath;
    }

    @JsonProperty("python_script")
    public String getPythonScriptHdfsPath() {
        return pythonScriptHdfsPath;
    }

    @JsonProperty("python_script")
    public void setPythonScriptHdfsPath(String pythonScriptHdfsPath) {
        this.pythonScriptHdfsPath = pythonScriptHdfsPath;
    }

    @JsonProperty("features")
    public List<String> getFeatures() {
        return features;
    }

    @JsonProperty("features")
    public void setFeatures(List<String> features) {
        this.features = features;
    }

    public void addFeature(String feature) {
        features.add(feature);
    }

    @JsonProperty("targets")
    public List<String> getTargets() {
        return targets;
    }

    @JsonProperty("targets")
    public void setTargets(List<String> targets) {
        this.targets = targets;
    }

    public void addTarget(String target) {
        targets.add(target);
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @JsonProperty("model_data_dir")
    public String getModelHdfsDir() {
        return modelHdfsDir;
    }

    @JsonProperty("model_data_dir")
    public void setModelHdfsDir(String modelHdfsDir) {
        this.modelHdfsDir = modelHdfsDir;
    }

    @JsonProperty("schema")
    public String getSchemaHdfsPath() {
        return schemaHdfsPath;
    }

    @JsonProperty("schema")
    public void setSchemaHdfsPath(String schemaHdfsPath) {
        this.schemaHdfsPath = schemaHdfsPath;
    }

    @JsonProperty("data_format")
    public String getDataFormat() {
        return dataFormat;
    }

    @JsonProperty("data_format")
    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    @JsonProperty("algorithm_properties")
    public String getAlgorithmProperties() {
        return algorithmProperties;
    }

    @JsonProperty("algorithm_properties")
    public void setAlgorithmProperties(String algorithmProperties) {
        this.algorithmProperties = algorithmProperties;
    }

    @JsonProperty("key_columns")
	public List<String> getKeyCols() {
		return keyCols;
	}

    @JsonProperty("key_columns")
	public void setKeyCols(List<String> keyCols) {
		this.keyCols = keyCols;
	}

}
