package com.latticeengines.dataplatform.exposed.domain;

import java.util.ArrayList;
import java.util.List;

import org.codehaus.jackson.annotate.JsonIgnore;
import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.dataplatform.util.JsonHelper;

public class Model implements HasName, HasId<String> {

    private String id;
    private String name;
    private String dataHdfsPath;
    private String schemaHdfsPath;
    private String modelHdfsDir;
    private List<String> features;
    private List<String> targets;
    private ModelDefinition modelDefinition;
    private List<Job> jobs = new ArrayList<Job>();
    private String dataFormat;
	private String customer;
	private String table;

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

    @JsonProperty("model_definition")
    public ModelDefinition getModelDefinition() {
        return modelDefinition;
    }

    @JsonProperty("model_definition")
    public void setModelDefinition(ModelDefinition modelDefinition) {
        this.modelDefinition = modelDefinition;
    }

    @JsonProperty("features")
    public List<String> getFeatures() {
        return features;
    }

    @JsonProperty("features")
    public void setFeatures(List<String> features) {
        this.features = features;
    }

    @JsonProperty("targets")
    public List<String> getTargets() {
        return targets;
    }

    @JsonProperty("targets")
    public void setTargets(List<String> targets) {
        this.targets = targets;
    }

    @JsonProperty("data")
    public String getDataHdfsPath() {
        return dataHdfsPath;
    }

    @JsonProperty("data")
    public void setDataHdfsPath(String dataHdfsPath) {
        this.dataHdfsPath = dataHdfsPath;
    }

    @JsonProperty("schema")
    public String getSchemaHdfsPath() {
        return schemaHdfsPath;
    }

    @JsonProperty("schema")
    public void setSchemaHdfsPath(String schemaHdfsPath) {
        this.schemaHdfsPath = schemaHdfsPath;
    }

    @JsonProperty("model_dir_data")
    public String getModelHdfsDir() {
        return modelHdfsDir;
    }

    @JsonProperty("model_dir_data")
    public void setModelHdfsDir(String modelHdfsDir) {
        this.modelHdfsDir = modelHdfsDir;
    }

    @Override
    public String toString() {
        return JsonHelper.serialize(this);
    }

    @Override
    @JsonIgnore
    public String getId() {
        return id;
    }

    @Override
    @JsonIgnore
    public void setId(String id) {
        this.id = id;
        if (id == null) {
            jobs = new ArrayList<Job>();
        }
    }

    public List<Job> getJobs() {
        return jobs;
    }

    public void addJob(Job job) {
        jobs.add(job);
    }

    @JsonProperty("data_format")
    public String getDataFormat() {
        return dataFormat;
    }

    @JsonProperty("data_format")
    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }
    
    @JsonProperty("customer")
    public void setCustomer(String customer) {
    	this.customer = customer;
    }
    
    public String getCustomer() {
    	return customer;
    }

    @JsonProperty("table")
	public String getTable() {
		return table;
	}

    @JsonProperty("table")
	public void setTable(String table) {
		this.table = table;
	}
    
    @JsonIgnore
    public String getSampleHdfsPath() {
        return getDataHdfsPath() + "/samples";
    }

}
