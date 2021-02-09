package com.latticeengines.domain.exposed.spark.cdl;

import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.latticeengines.domain.exposed.cdl.activity.DeriveConfig;
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

    @JsonInclude(JsonInclude.Include.NON_NULL)
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "name")
    @JsonSubTypes({
            @JsonSubTypes.Type(value = Dimension.class, name = Dimension.NAME),
            @JsonSubTypes.Type(value = DerivedDimension.class, name = DerivedDimension.NAME)
    })
    public static class Dimension {
        public static final String NAME = "Dimension";

        @JsonProperty
        public int inputIdx;
        @JsonProperty
        public Map<String, String> renameAttrs;
        @JsonProperty
        public Map<String, String> hashAttrs; // hash key to value
        @JsonProperty
        public Set<String> attrs;
        @JsonProperty
        public Set<String> dedupAttrs;
        @JsonProperty
        public Integer valueLimit;
    }

    public static class DerivedDimension extends Dimension {
        public static final String NAME = "DerivedDimension";

        @JsonProperty
        public DeriveConfig deriveConfig;
    }
}
