package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class ExportTimelineJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;
    public static final String NAME = "exportTimelineJob";

    @JsonProperty
    public Map<String, Integer> inputIdx = new HashMap<>();
    @JsonProperty
    public Integer latticeAccountTableIdx;
    @JsonProperty
    public Integer timelineUniverseAccountListIdx;
    //timelineId -> timelineTableName
    @JsonProperty
    public Map<String, String> timelineTableNames;
    @JsonProperty
    public Long fromDateTimestamp;
    @JsonProperty
    public Long toDateTimestamp;
    @JsonProperty
    public boolean rollupToDaily;
    @JsonProperty
    public List<String> eventTypes = new ArrayList<>();
    @JsonProperty
    public List<String> accountIdList = new ArrayList<>();

    @Override
    public int getNumTargets() {
        return MapUtils.isEmpty(timelineTableNames) ? 0 : timelineTableNames.size();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
