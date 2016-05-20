package com.latticeengines.domain.exposed.scoring;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.BasePayloadConfiguration;
import com.latticeengines.domain.exposed.metadata.Table;

public class RTSBulkScoringConfiguration extends BasePayloadConfiguration {

    private Table metadataTable;

    private Map<String, String> properties = new HashMap<>();

    @JsonProperty("properties")
    public Map<String, String> getProperties() {
        return this.properties;
    }

    @JsonProperty("properties")
    public void setProperties(Map<String, String> properties) {
        this.properties = properties;
    }

    @JsonProperty("metadata_table")
    public Table getMetadataTable() {
        return this.metadataTable;
    }

    @JsonProperty("metadata_table")
    public void setMetadataTable(Table metadataTable) {
        this.metadataTable = metadataTable;
    }

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

}
