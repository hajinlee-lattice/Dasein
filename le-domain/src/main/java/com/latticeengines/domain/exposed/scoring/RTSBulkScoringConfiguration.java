package com.latticeengines.domain.exposed.scoring;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;

public class RTSBulkScoringConfiguration extends BasePayloadConfiguration {

    private Table metadataTable;

    private String targetResultDir;

    private boolean enableLeadEnrichment;

    private boolean enableDebug;

    private boolean scoreTestFile;

    private String internalResourceHostPort;

    private List<String> modelGuids = new ArrayList<>();

    private String modelType;

    private String importErrorPath;

    @JsonProperty("model_guids")
    public List<String> getModelGuids() {
        return this.modelGuids;
    }

    @JsonProperty("model_guids")
    public void setModelGuids(List<String> modelGuids) {
        this.modelGuids = modelGuids;
    }

    @JsonProperty("metadata_table")
    public Table getMetadataTable() {
        return this.metadataTable;
    }

    @JsonProperty("metadata_table")
    public void setMetadataTable(Table metadataTable) {
        this.metadataTable = metadataTable;
    }

    @JsonProperty("enable_lead_enrichment")
    public boolean isEnableLeadEnrichment() {
        return this.enableLeadEnrichment;
    }

    @JsonProperty("enable_lead_enrichment")
    public void setEnableLeadEnrichment(boolean enableLeadEnrichment) {
        this.enableLeadEnrichment = enableLeadEnrichment;
    }

    @JsonProperty("enable_debug")
    public void setEnableDebug(boolean enableDebug) {
        this.enableDebug = enableDebug;
    }

    @JsonProperty("enable_debug")
    public boolean isEnableDebug() {
        return this.enableDebug;
    }

    @JsonProperty("target_result_dir")
    public String getTargetResultDir() {
        return targetResultDir;
    }

    @JsonProperty("target_result_dir")
    public void setTargetResultDir(String targetResultDir) {
        this.targetResultDir = targetResultDir;
    }

    @JsonProperty("internal_resource_host_port")
    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

    @JsonProperty("internal_resource_host_port")
    public String getInternalResourceHostPort() {
        return this.internalResourceHostPort;
    }

    @JsonProperty("import_error_path")
    public String getImportErrorPath() {
        return importErrorPath;
    }

    @JsonProperty("import_error_path")
    public void setImportErrorPath(String importErrorPath) {
        this.importErrorPath = importErrorPath;
    }

    @JsonProperty("model_type")
    public void setModelType(String modelType) {
        this.modelType = modelType;
    }

    @JsonProperty("model_type")
    public String getModelType() {
        return this.modelType;
    }

    @JsonProperty("score_test_file")
    public void setScoreTestFile(boolean scoreTestFile) {
        this.scoreTestFile = scoreTestFile;
    }

    @JsonProperty("score_test_file")
    public boolean getScoreTestFile() {
        return this.scoreTestFile;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
