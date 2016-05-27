package com.latticeengines.domain.exposed.scoring;

import java.util.ArrayList;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;

public class RTSBulkScoringConfiguration extends BasePayloadConfiguration {

    private String tenant;

    private Table metadataTable;

    private String targetResultDir;

    private List<String> modelGuids = new ArrayList<>();

    @JsonProperty("tenant")
    public String getTenant() {
        return this.tenant;
    }

    @JsonProperty("tenant")
    public void setTenant(String tenant) {
        this.tenant = tenant;
    }

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

    @JsonProperty("target_result_dir")
    public String getTargetResultDir() {
        return targetResultDir;
    }

    @JsonProperty("target_result_dir")
    public void setTargetResultDir(String targetResultDir) {
        this.targetResultDir = targetResultDir;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
