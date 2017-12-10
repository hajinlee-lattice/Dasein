package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.VdbLoadTableConfig;

@JsonIgnoreProperties(ignoreUnknown = true)
public class VdbImportConfig extends CDLImportConfig {

    @JsonProperty("vdb_load_table_config")
    private VdbLoadTableConfig vdbLoadTableConfig;

    public VdbLoadTableConfig getVdbLoadTableConfig() {
        return vdbLoadTableConfig;
    }

    public void setVdbLoadTableConfig(VdbLoadTableConfig vdbLoadTableConfig) {
        this.vdbLoadTableConfig = vdbLoadTableConfig;
    }
}
