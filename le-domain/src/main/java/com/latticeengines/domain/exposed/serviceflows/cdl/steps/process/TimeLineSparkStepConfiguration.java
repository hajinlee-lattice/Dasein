package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.S3ImportSystem;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.cdl.activity.TimeLine;
import com.latticeengines.domain.exposed.serviceflows.core.steps.SparkJobStepConfiguration;

public class TimeLineSparkStepConfiguration extends SparkJobStepConfiguration {

    @JsonProperty("timeline_list")
    private List<TimeLine> timeLineList;

    @JsonProperty("rebuild_timeline")
    private boolean shouldRebuild;

    // streamId -> stream object
    @JsonProperty("activity_stream_map")
    private Map<String, AtlasStream> activityStreamMap;

    @JsonProperty("template_to_systemtype_map")
    private Map<String, S3ImportSystem.SystemType> templateToSystemTypeMap;

    public List<TimeLine> getTimeLineList() {
        return timeLineList;
    }

    public void setTimeLineList(List<TimeLine> timeLineList) {
        this.timeLineList = timeLineList;
    }

    public boolean isShouldRebuild() {
        return shouldRebuild;
    }

    public void setShouldRebuild(boolean shouldRebuild) {
        this.shouldRebuild = shouldRebuild;
    }

    public Map<String, AtlasStream> getActivityStreamMap() {
        return activityStreamMap;
    }

    public void setActivityStreamMap(Map<String, AtlasStream> activityStreamMap) {
        this.activityStreamMap = activityStreamMap;
    }

    public Map<String, S3ImportSystem.SystemType> getTemplateToSystemTypeMap() {
        return templateToSystemTypeMap;
    }

    public void setTemplateToSystemTypeMap(Map<String, S3ImportSystem.SystemType> templateToSystemTypeMap) {
        this.templateToSystemTypeMap = templateToSystemTypeMap;
    }
}
