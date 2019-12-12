package com.latticeengines.domain.exposed.datacloud.transformation.config.atlas;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class ConsolidateCollectionConfig extends TransformerConfig {

    @JsonProperty("Vendor")
    private String vendor;

    @JsonProperty("RawIngestion")
    private String rawIngestion;

    public String getVendor() {
        return vendor;
    }

    public void setVendor(String vendor) {
        this.vendor = vendor;
    }

    public String getRawIngestion() {
        return rawIngestion;
    }

    public void setRawIngestion(String rawIngestion) {
        this.rawIngestion = rawIngestion;
    }
}
