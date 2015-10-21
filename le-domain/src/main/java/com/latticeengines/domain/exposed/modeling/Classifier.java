package com.latticeengines.domain.exposed.modeling;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.dataplatform.HasName;

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
    private String provenanceProperties;
    private String dataProfileHdfsPath;
    private String configMetadataHdfsPath;
    private String dataDiagnosticsPath;
    private String pythonPipelineLibHdfsPath;
    private String pythonPipelineScriptHdfsPath;
    private String pythonMRConfigHdfsPath;
    
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

    @JsonProperty("provenance_properties")
    public String getProvenanceProperties() {
        return provenanceProperties;
    }

    @JsonProperty("provenance_properties")
    public void setProvenanceProperties(String provenanceProperties) {
        this.provenanceProperties = provenanceProperties;
    }

    @JsonProperty("key_columns")
    public List<String> getKeyCols() {
        return keyCols;
    }

    @JsonProperty("key_columns")
    public void setKeyCols(List<String> keyCols) {
        this.keyCols = keyCols;
    }

    @JsonProperty("data_profile")
    public String getDataProfileHdfsPath() {
        return dataProfileHdfsPath;
    }

    @JsonProperty("data_profile")
    public void setDataProfileHdfsPath(String dataProfileHdfsPath) {
        this.dataProfileHdfsPath = dataProfileHdfsPath;
    }

    @JsonProperty("config_metadata")
    public String getConfigMetadataHdfsPath() {
        return configMetadataHdfsPath;
    }

    @JsonProperty("config_metadata")
    public void setConfigMetadataHdfsPath(String configMetadataHdfsPath) {
        this.configMetadataHdfsPath = configMetadataHdfsPath;
    }

    @JsonProperty("diagnostics_path")
    public String getDataDiagnosticsPath() {
        return dataDiagnosticsPath;
    }

    @JsonProperty("diagnostics_path")
    public void setDataDiagnosticsPath(String dataDiagnosticsPath) {
        this.dataDiagnosticsPath = dataDiagnosticsPath;
    }

    @JsonProperty("python_pipeline_script")
    public String getPythonPipelineScriptHdfsPath() {
        return pythonPipelineScriptHdfsPath;
    }

    @JsonProperty("python_pipeline_script")
    public void setPythonPipelineScriptHdfsPath(String pythonPipelineScriptHdfsPath) {
        this.pythonPipelineScriptHdfsPath = pythonPipelineScriptHdfsPath;
    }

    @JsonProperty("python_pipeline_lib")
    public String getPythonPipelineLibHdfsPath() {
        return pythonPipelineLibHdfsPath;
    }

    @JsonProperty("python_pipeline_lib")
    public void setPythonPipelineLibHdfsPath(String pythonPipelineLibHdfsPath) {
        this.pythonPipelineLibHdfsPath = pythonPipelineLibHdfsPath;
    }

    @JsonProperty("python_mr_config")
    public String getPythonMRConfigHdfsPath() {
        return pythonMRConfigHdfsPath;
    }

    @JsonProperty("python_mr_config")
    public void setPythonMRConfigHdfsPath(String pythonMRConfigHdfsPath) {
        this.pythonMRConfigHdfsPath = pythonMRConfigHdfsPath;
    }
    
}
