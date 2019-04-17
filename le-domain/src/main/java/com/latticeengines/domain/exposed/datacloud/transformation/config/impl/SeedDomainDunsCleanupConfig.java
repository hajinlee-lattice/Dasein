package com.latticeengines.domain.exposed.datacloud.transformation.config.impl;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SeedDomainDunsCleanupConfig extends TransformerConfig {
    @JsonProperty("GoldenDomainField")
    private String goldenDomainField;

    @JsonProperty("GoldenDunsField")
    private String goldenDunsField;

    @JsonProperty("SeedDomainField")
    private String seedDomainField;

    @JsonProperty("SeedDunsField")
    private String seedDunsField;

    public String getGoldenDomainField() {
        return goldenDomainField;
    }

    public void setGoldenDomainField(String goldenDomainField) {
        this.goldenDomainField = goldenDomainField;
    }

    public String getGoldenDunsField() {
        return goldenDunsField;
    }

    public void setGoldenDunsField(String goldenDunsField) {
        this.goldenDunsField = goldenDunsField;
    }

    public String getSeedDomainField() {
        return seedDomainField;
    }

    public void setSeedDomainField(String seedDomainField) {
        this.seedDomainField = seedDomainField;
    }

    public String getSeedDunsField() {
        return seedDunsField;
    }

    public void setSeedDunsField(String seedDunsField) {
        this.seedDunsField = seedDunsField;
    }
}
