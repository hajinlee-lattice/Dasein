package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.ActivityRowReducer;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class AggDailyActivityConfig extends SparkJobConfig {

    public static final String NAME = "aggDailyActivity";
    private static final long serialVersionUID = 2423144672398876823L;

    // streamId -> date attribute name
    @JsonProperty
    public Map<String, String> streamDateAttrs = new HashMap<>();

    // streamId -> reducer
    @JsonProperty
    public Map<String, ActivityRowReducer> streamReducerMap = new HashMap<>();

    // streamId -> dimensionName -> metadata
    @JsonProperty
    public Map<String, Map<String, DimensionMetadata>> dimensionMetadataMap = new HashMap<>();

    // streamId -> set(dimensionName)
    @JsonProperty
    public Map<String, Set<String>> hashDimensionMap = new HashMap<>();

    // streamId -> dimensionName -> calculator
    @JsonProperty
    public Map<String, Map<String, DimensionCalculator>> dimensionCalculatorMap = new HashMap<>();

    // streamId -> List[attribute deriver]
    @JsonProperty
    public Map<String, List<StreamAttributeDeriver>> attrDeriverMap = new HashMap<>();

    // dimension value -> short ID
    @JsonProperty
    public Map<String, String> dimensionValueIdMap = new HashMap<>();

    // streamId -> List[dimension attr col name already generated]
    @JsonProperty
    public Map<String, List<String>> additionalDimAttrMap = new HashMap<>();

    @JsonProperty
    public Set<String> incrementalStreams = new HashSet<>();

    // two tables in each input section if incremental stream, first one raw stream import, second existing batch
    @JsonProperty
    public ActivityStoreSparkIOMetadata inputMetadata;

    @JsonProperty
    public Map<String, Integer> streamRetentionDays = new HashMap<>();

    @JsonProperty
    public Long currentEpochMilli;

    @Override
    public int getNumTargets() {
        if (MapUtils.isEmpty(inputMetadata.getMetadata())) {
            return 0;
        }
        // for normal streams: 1 new batch store
        // for incremental streams: 1 updated batch store + 1 delta daily stream
        return inputMetadata.getMetadata().keySet().stream().mapToInt(streamId -> incrementalStreams.contains(streamId) ? 2 : 1).sum();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
