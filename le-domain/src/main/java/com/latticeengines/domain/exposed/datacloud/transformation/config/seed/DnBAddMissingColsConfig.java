package com.latticeengines.domain.exposed.datacloud.transformation.config.seed;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenFuncConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenTransformerConfig;

public class DnBAddMissingColsConfig extends TblDrivenTransformerConfig {

    @JsonProperty("LE_Domain")
    private String domain;

    @JsonProperty("DUNS_NUMBER")
    private String duns;

    @JsonProperty("Seed")
    private String seed;

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

    public String getSeed() {
        return seed;
    }

    public void setSeed(String seed) {
        this.seed = seed;
    }

    public static class MapFunc extends TblDrivenFuncConfig {

        @JsonProperty("Source")
        String source;

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

    }

}
