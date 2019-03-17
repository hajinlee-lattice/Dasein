package com.latticeengines.domain.exposed.datacloud.transformation.configuration.seed;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;

public class SeedDomainOnlyCleanupConfig extends TransformerConfig {
    // source name -> domain field
    @JsonProperty("SrcDomains")
    private Map<String, String> srcDomains;

    @JsonProperty("SeedDomain")
    private String seedDomain;

    @JsonProperty("SeedDuns")
    private String seedDuns;

    public Map<String, String> getSrcDomains() {
        return srcDomains;
    }

    public void setSrcDomains(Map<String, String> srcDomains) {
        this.srcDomains = srcDomains;
    }

    public String getSeedDomain() {
        return seedDomain;
    }

    public void setSeedDomain(String seedDomain) {
        this.seedDomain = seedDomain;
    }

    public String getSeedDuns() {
        return seedDuns;
    }

    public void setSeedDuns(String seedDuns) {
        this.seedDuns = seedDuns;
    }

}
