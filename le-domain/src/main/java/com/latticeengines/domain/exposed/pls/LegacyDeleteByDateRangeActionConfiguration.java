package com.latticeengines.domain.exposed.pls;

import java.util.Date;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public class LegacyDeleteByDateRangeActionConfiguration extends ActionConfiguration {

    @JsonProperty("start_time")
    private Date startTime;

    @JsonProperty("end_time")
    private Date endTime;

    @JsonProperty("entity")
    private BusinessEntity entity;

    @Override
    public String toString() {
        return JsonUtils.serialize(this);
    }

    @Override
    public String serialize() {
        return toString();
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public void setEntity(BusinessEntity entity) {
        this.entity = entity;
    }

    public BusinessEntity getEntity() {
        return entity;
    }
}
