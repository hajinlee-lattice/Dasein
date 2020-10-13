package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class GenerateTimelineExportArtifactsJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;
    public static final String NAME = "generateTimelineExportArtifacts";

    @JsonProperty
    public Map<String, Integer> inputIdx = new HashMap<>();
    @JsonProperty
    public Integer latticeAccountTableIdx;
    //using to filter timelineTable.
    @JsonProperty
    public Integer accountListIdx;
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
    public String timeZone;
    @JsonProperty
    public boolean filterDuns;
    @JsonProperty
    public boolean includeOrphan;
    @JsonProperty
    public List<String> eventTypes = new ArrayList<>();

    @Override
    public int getNumTargets() {
        return MapUtils.isEmpty(timelineTableNames) ? 0 : timelineTableNames.size();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
