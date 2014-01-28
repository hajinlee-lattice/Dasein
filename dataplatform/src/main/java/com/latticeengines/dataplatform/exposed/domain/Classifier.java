package com.latticeengines.dataplatform.exposed.domain;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.util.JsonHelper;

public class Classifier implements HasName {

	private String name;
	private DataSchema schema;
	private List<Field> features = new ArrayList<Field>();
	private List<Field> targets = new ArrayList<Field>();
	
	@Override
	@JsonProperty("name")
	public String getName() {
		return name;
	}

	@Override
	public void setName(String name) {
		this.name = name;
	}

	@JsonProperty("schema")
	public DataSchema getSchema() {
		return schema;
	}

	public void setSchema(DataSchema schema) {
		this.schema = schema;
	}

	@JsonProperty("features")
	public List<Field> getFeatures() {
		return features;
	}

	public void addFeatures(Field feature) {
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
