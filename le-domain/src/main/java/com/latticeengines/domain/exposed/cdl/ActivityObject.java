package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.pls.Action;

public class ActivityObject {

    private String tenantName;
    private Boolean scheduleNow;
    private Date scheduleTime;
    private List<Action> actions;
    private Boolean dataCloudRefresh;
    private Date invokeTime;

    public String getTenantName() { return tenantName; }

    public void setTenantName(String tenantName) { this.tenantName = tenantName; }

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
}
