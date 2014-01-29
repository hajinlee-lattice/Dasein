package com.latticeengines.dataplatform.exposed.domain;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.util.JsonHelper;

public class Classifier implements HasName {

	private String name;
	private String trainingDataHdfsPath;
	private String testDataHdfsPath;
	private String pythonScriptHdfsPath;
	private DataSchema schema;
	private List<Field> features = new ArrayList<Field>();
	private List<Field> targets = new ArrayList<Field>();
	
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

	@JsonProperty("schema")
	public DataSchema getSchema() {
		return schema;
	}

	@JsonProperty("schema")
	public void setSchema(DataSchema schema) {
		this.schema = schema;
	}

	@JsonProperty("features")
	public List<Field> getFeatures() {
		return features;
	}

	public void addFeature(Field feature) {
		features.add(feature);
	}

	@JsonProperty("targets")
	public List<Field> getTargets() {
		return targets;
	}

	public void addTarget(Field target) {
		targets.add(target);
	}
	
	@Override
	public String toString() {
		return JsonHelper.serialize(this);
	}

}
