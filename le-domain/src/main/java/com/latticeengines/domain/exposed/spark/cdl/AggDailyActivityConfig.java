package com.latticeengines.domain.exposed.spark.cdl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.DimensionCalculator;
import com.latticeengines.domain.exposed.cdl.activity.DimensionMetadata;
import com.latticeengines.domain.exposed.cdl.activity.StreamAttributeDeriver;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class AggDailyActivityConfig extends SparkJobConfig {

    public static final String NAME = "aggDailyActivity";
    private static final long serialVersionUID = 2423144672398876823L;

    @JsonProperty
    public Map<String, Integer> rawStreamInputIdx = new HashMap<>();

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

    // streamId -> List[dimension attr col name already generated]
    @JsonProperty
    public Map<String, List<String>> additionalDimAttrMap = new HashMap<>();

    @Override
    public int getNumTargets() {
        return MapUtils.isEmpty(rawStreamInputIdx) ? 0 : rawStreamInputIdx.size();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
