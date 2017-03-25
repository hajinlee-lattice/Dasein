package com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;

public class MapAttributeConfig extends TblDrivenTransformerConfig {

    @JsonProperty("Seed")
    private String seed;

    @JsonProperty("SeedId")
    private String seedId;

    @JsonProperty("JoinConfigs")
    private List<JoinConfig> joinConfigs;

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

    public List<JoinConfig> getJoinConfigs() {
        return joinConfigs;
    }

    public void setJoinConfigs(List<JoinConfig> joinConfigs) {
        this.joinConfigs = joinConfigs;
    }

    static public class JoinTarget {

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

    static public class JoinConfig {

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

    static public class MapFunc extends TblDrivenFuncConfig {

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
