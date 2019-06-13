package com.latticeengines.domain.exposed.cdl;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.security.TenantType;

public class ScheduleNowPriorityObject extends PriorityObject {

    @JsonProperty("schedule_time")
    private Long scheduleTime;

    @Override
    public int compareTo(PriorityObject o) {
        int superResult = super.compareTo(o);
        if (superResult != 0) {
            return superResult;
        }
        return compare((ScheduleNowPriorityObject) o);
    }

    public int compare(ScheduleNowPriorityObject o) {
        return o.getScheduleTime() - this.scheduleTime > 0 ? -1 : 1;
    }

    public ScheduleNowPriorityObject(TenantType tenantType) {
        this.tenantType = tenantType;
    }

    public Long getScheduleTime() {
        return scheduleTime;
    }

    public void setScheduleTime(Long scheduleTime) {
        this.scheduleTime = scheduleTime;
    }

}
