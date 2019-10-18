package com.latticeengines.domain.exposed.serviceflows.cdl.steps.process;

import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.cdl.activity.AtlasStream;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class ProcessActivityStreamStepConfiguration extends BaseProcessEntityStepConfiguration {

    // streamId -> stream object
    @JsonProperty("activity_stream_map")
    private Map<String, AtlasStream> activityStreamMap;

    @JsonProperty("entity_match_enabled")
    private boolean entityMatchEnabled;

    public Map<String, AtlasStream> getActivityStreamMap() {
        return activityStreamMap;
    }

    public void setActivityStreamMap(Map<String, AtlasStream> activityStreamMap) {
        this.activityStreamMap = activityStreamMap;
    }

    public boolean isEntityMatchEnabled() {
        return entityMatchEnabled;
    }

    public void setEntityMatchEnabled(boolean entityMatchEnabled) {
        this.entityMatchEnabled = entityMatchEnabled;
    }

    @Override
    public BusinessEntity getMainEntity() {
        return BusinessEntity.ActivityStream;
    }
}
