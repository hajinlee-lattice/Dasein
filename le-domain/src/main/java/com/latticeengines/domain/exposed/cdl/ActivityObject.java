package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.security.Tenant;

public class ActivityObject {

    private Tenant tenant;
    private Boolean scheduleNow;
    private Date scheduleTime;
    private List<Action> actions;
    private Boolean dataCloudRefresh;
    private Date invokeTime;
    private Boolean retry;

    public Tenant getTenant() { return tenant; }

    public void setTenant(Tenant tenant) { this.tenant = tenant; }

    public Boolean getScheduleNow() { return scheduleNow; }

    public void setScheduleNow(Boolean scheduleNow) { this.scheduleNow = scheduleNow; }

    public Date getScheduleTime() { return scheduleTime; }

    public void setScheduleTime(Date scheduleTime) { this.scheduleTime = scheduleTime; }

    public List<Action> getActions() { return actions; }

    public void setActions(List<Action> actions) { this.actions = actions; }

    public Boolean getDataCloudRefresh() { return dataCloudRefresh; }

    public void setDataCloudRefresh(Boolean dataCloudRefresh) { this.dataCloudRefresh = dataCloudRefresh; }

    public Date getInvokeTime() { return invokeTime; }

    public void setInvokeTime(Date invokeTime) { this.invokeTime = invokeTime; }

    public Boolean getRetry() {
        return retry;
    }

    public void setRetry(Boolean retry) {
        this.retry = retry;
    }
}
