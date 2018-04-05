package com.latticeengines.domain.exposed.scoring;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;

public class RTSBulkScoringConfiguration extends BasePayloadConfiguration {

    @JsonProperty("metadata_table")
    private Table metadataTable;

    @JsonProperty("target_result_dir")
    private String targetResultDir;

    @JsonProperty("enable_lead_enrichment")
    private boolean enableLeadEnrichment;

    @JsonProperty("enable_debug")
    private boolean enableDebug;

    @JsonProperty("enable_matching")
    private boolean enableMatching = Boolean.TRUE;

    @JsonProperty("score_test_file")
    private boolean scoreTestFile;

    @JsonProperty("internal_resource_host_port")
    private String internalResourceHostPort;

    @JsonProperty("model_guids")
    private List<String> modelGuids = new ArrayList<>();

    @JsonProperty("model_type")
    private String modelType;

    @JsonProperty("import_error_path")
    private String importErrorPath;

    @JsonProperty("id_column_name")
    private String idColumnName;

    public List<String> getModelGuids() {
        return this.modelGuids;
    }

    public void setModelGuids(List<String> modelGuids) {
        this.modelGuids = modelGuids;
    }

    public Table getMetadataTable() {
        return this.metadataTable;
    }

    public void setMetadataTable(Table metadataTable) {
        this.metadataTable = metadataTable;
    }

    public boolean isEnableLeadEnrichment() {
        return this.enableLeadEnrichment;
    }

    public void setEnableLeadEnrichment(boolean enableLeadEnrichment) {
        this.enableLeadEnrichment = enableLeadEnrichment;
    }

    public void setEnableDebug(boolean enableDebug) {
        this.enableDebug = enableDebug;
    }

    public boolean isEnableDebug() {
        return this.enableDebug;
    }

    public boolean isEnableMatching() {
        return enableMatching;
    }

    public void setEnableMatching(boolean enableMatching) {
        this.enableMatching = enableMatching;
    }

    public String getTargetResultDir() {
        return targetResultDir;
    }

    public void setTargetResultDir(String targetResultDir) {
        this.targetResultDir = targetResultDir;
    }

    public void setInternalResourceHostPort(String internalResourceHostPort) {
        this.internalResourceHostPort = internalResourceHostPort;
    }

    public String getInternalResourceHostPort() {
        return this.internalResourceHostPort;
    }

    public String getImportErrorPath() {
        return importErrorPath;
    }

    public void setImportErrorPath(String importErrorPath) {
        this.importErrorPath = importErrorPath;
    }

    public void setModelType(String modelType) {
        this.modelType = modelType;
    }

    public String getModelType() {
        return this.modelType;
    }

    public void setScoreTestFile(boolean scoreTestFile) {
        this.scoreTestFile = scoreTestFile;
    }

    public boolean getScoreTestFile() {
        return this.scoreTestFile;
    }

    public String getIdColumnName() {
        return idColumnName;
    }

    public void setIdColumnName(String idColumnName) {
        this.idColumnName = idColumnName;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
