package com.latticeengines.domain.exposed.datacloud.transformation.config.am;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenFuncConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenTransformerConfig;

public class MapAttributeConfig extends TblDrivenTransformerConfig {

    @JsonProperty("DataCloudVersion")
    private String dataCloudVersion; // For AccountMaster rebuild/refresh

    @JsonProperty("IsMiniDataCloud")
    private Boolean isMiniDataCloud; // For AccountMaster rebuild/refresh

    @JsonProperty("Seed")
    private String seed;

    @JsonProperty("SeedId")
    private String seedId;

    @JsonProperty("JoinConfigs")
    private List<JoinConfig> joinConfigs;

    @JsonProperty("IsDedupe")
    private Boolean isDedupe = Boolean.TRUE;
    
    public String getDataCloudVersion() {
        return dataCloudVersion;
    }

    public void setDataCloudVersion(String dataCloudVersion) {
        this.dataCloudVersion = dataCloudVersion;
    }

    public Boolean isMiniDataCloud() {
        return isMiniDataCloud;
    }

    public void setIsMiniDataCloud(Boolean isMiniDataCloud) {
        this.isMiniDataCloud = isMiniDataCloud;
    }

    public String getSeed() {
        return seed;
    }

    public void setSeed(String seed) {
        this.seed = seed;
    }

    public String getSeedId() {
        return seedId;
    }

    public void setSeedId(String seedId) {
        this.seedId = seedId;
    }

    public Boolean getIsDedupe() {
        return isDedupe;
    }

    public void setIsDedupe(Boolean isDedupe) {
        this.isDedupe = isDedupe;
    }

    public List<JoinConfig> getJoinConfigs() {
        return joinConfigs;
    }

    public void setJoinConfigs(List<JoinConfig> joinConfigs) {
        this.joinConfigs = joinConfigs;
    }

    public static class JoinTarget {

        @JsonProperty("Keys")
        List<String> keys;

        @JsonProperty("Source")
        String source;

        public List<String> getKeys() {
            return keys;
        }

        public void setKeys(List<String> keys) {
            this.keys = keys;
        }

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }
    }

    public static class JoinConfig {

        @JsonProperty("Keys")
        List<String> keys;

        @JsonProperty("Targets")
        List<JoinTarget> targets;

        public List<String> getKeys() {
            return keys;
        }

        public void setKeys(List<String> keys) {
            this.keys = keys;
        }

        public List<JoinTarget> getTargets() {
            return targets;
        }

        public void setTargets(List<JoinTarget> targets) {
            this.targets = targets;
        }
    }

    public static class MapFunc extends TblDrivenFuncConfig {

        @JsonProperty("Source")
        String source;

        @JsonProperty("attribute")
        String attribute;

        public String getSource() {
            return source;
        }

        public void setSource(String source) {
            this.source = source;
        }

        public String getAttribute() {
            return attribute;
        }

        public void setAttribute(String attribute) {
            this.attribute = attribute;
        }
    }
}
