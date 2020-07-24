package com.latticeengines.domain.exposed.datacloud.transformation.config.seed;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;

public class DnBAddMissingColsConfig extends TransformerConfig {

    @JsonProperty("LE_Domain")
    private String domain;

    @JsonProperty("DUNS_NUMBER")
    private String duns;

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }
}
