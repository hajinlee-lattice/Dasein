package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

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
