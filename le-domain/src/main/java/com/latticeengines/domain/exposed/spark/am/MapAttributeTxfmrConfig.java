package com.latticeengines.domain.exposed.spark.am;

import java.io.Serializable;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.datacloud.manage.SourceAttribute;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TblDrivenFuncConfig;

public class MapAttributeTxfmrConfig extends TblDrivenTxfmrConfig implements Serializable {

    public static final String NAME = "mapAttributeTxfmr";

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

    @JsonProperty("IsFilterChange")
    private Boolean isFilterChange = Boolean.TRUE;

    @JsonProperty("TimestampField")
    private String timestampField;

    // From DB
    @JsonProperty("SrcAttrs")
    private List<SourceAttribute> srcAttrs;

    @JsonProperty("MapFuncs")
    private List<MapFunc> mapFuncs;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

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

    public Boolean getIsFilterChange() {
        return isFilterChange;
    }

    public void setIsFilterChange(Boolean isFilterChange) {
        this.isFilterChange = isFilterChange;
    }

    public List<JoinConfig> getJoinConfigs() {
        return joinConfigs;
    }

    public void setJoinConfigs(List<JoinConfig> joinConfigs) {
        this.joinConfigs = joinConfigs;
    }

    public String getTimestampField() {
        return timestampField;
    }

    public void setTimestampField(String timestampField) {
        this.timestampField = timestampField;
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

    public List<SourceAttribute> getSrcAttrs() {
        return srcAttrs;
    }

    public void setSrcAttrs(List<SourceAttribute> srcAttrs) {
        this.srcAttrs = srcAttrs;
    }

    public List<MapFunc> getMapFuncs() {
        return mapFuncs;
    }

    public void setMapFuncs(List<MapFunc> mapFuncs) {
        this.mapFuncs = mapFuncs;
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
