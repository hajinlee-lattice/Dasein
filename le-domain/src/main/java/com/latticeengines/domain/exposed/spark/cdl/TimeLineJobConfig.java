package com.latticeengines.domain.exposed.spark.cdl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.commons.collections4.MapUtils;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class TimeLineJobConfig extends SparkJobConfig implements Serializable {

    private static final long serialVersionUID = 0L;
    public static final String NAME = "timelineJob";

    // index of contact table in input, use to retrieve contact -> account mapping
    @JsonProperty
    public Integer contactTableIdx;

    //streamTableName -> inputIdx
    @JsonProperty
    public Map<String, Integer> rawStreamInputIdx = new HashMap<>();

    //timelineObject -> (BusinessEntity, streamTableName)
    @JsonProperty
    public Map<String, Map<String, Set<String>>> timelineRelatedStreamTables;

    //timelineId -> timelineObject
    @JsonProperty
    public Map<String, TimeLine> timeLineMap = new HashMap<>();

    //streamTableName -> streamType
    @JsonProperty
    public Map<String, String> streamTypeWithTableNameMap;

    //timelineId -> timelineVersion
    @JsonProperty
    public Map<String, String> timelineVersionMap;

    @JsonProperty
    public String partitionKey;

    @JsonProperty
    public String sortKey;

    //templateName -> s3ImportSystemType
    @JsonProperty
    public Map<String, String> templateToSystemTypeMap;

    @Override
    public int getNumTargets() {
        return MapUtils.isEmpty(timelineRelatedStreamTables) ? 0 : timelineRelatedStreamTables.size();
    }

    @Override
    public String getName() {
        return NAME;
    }
}
