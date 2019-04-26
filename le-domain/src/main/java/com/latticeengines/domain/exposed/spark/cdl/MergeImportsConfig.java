package com.latticeengines.domain.exposed.spark.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class MergeImportsConfig extends SparkJobConfig {

    public static final String NAME = "mergeImports";

    // default join key for all rhs
    // if not specified, or not exists on both sides
    // concatenate instead of join
    @JsonProperty("JoinKey")
    private String joinKey;

    @JsonProperty("SrcId")
    private String srcId; // dedupe by srcId, then rename to join key

    @JsonProperty("DedupSrc")
    private boolean dedupSrc; // dedupe each input by srcId

    @JsonProperty("AddTimestamps")
    private boolean addTimestamps; // add cdl timestamp cols

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

    public static MergeImportsConfig joinBy(String joinKey) {
        MergeImportsConfig config = new MergeImportsConfig();
        config.setJoinKey(joinKey);
        return config;
    }

    public String getJoinKey() {
        return joinKey;
    }

    public void setJoinKey(String joinKey) {
        this.joinKey = joinKey;
    }

    public String getSrcId() {
        return srcId;
    }

    public void setSrcId(String srcId) {
        this.srcId = srcId;
    }

    public boolean isDedupSrc() {
        return dedupSrc;
    }

    public void setDedupSrc(boolean dedupSrc) {
        this.dedupSrc = dedupSrc;
    }

    public boolean isAddTimestamps() {
        return addTimestamps;
    }

    public void setAddTimestamps(boolean addTimestamps) {
        this.addTimestamps = addTimestamps;
    }
}
