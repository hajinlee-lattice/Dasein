package com.latticeengines.domain.exposed.spark.cdl;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ProcessDimensionConfig extends SparkJobConfig {

    public static final String NAME = "processDimension";
    private static final long serialVersionUID = 174485365513007792L;

    @JsonProperty
    public boolean collectMetadata;

    @JsonProperty
    public Map<String, Dimension> dimensions;

    @Override
    public String getName() {
        return NAME;
    }

    @Override
    public int getNumTargets() {
        return (collectMetadata || MapUtils.isEmpty(dimensions)) ? 0 : dimensions.size();
    }

    public static class Dimension {
        @JsonProperty
        public int inputIdx;
        @JsonProperty
        public Map<String, String> renameAttrs;
        @JsonProperty
        public Map<String, String> hashAttrs;
        @JsonProperty
        public Set<String> attrs;
        @JsonProperty
        public Set<String> dedupAttrs;
        @JsonProperty
        public Integer valueLimit;
    }
}
