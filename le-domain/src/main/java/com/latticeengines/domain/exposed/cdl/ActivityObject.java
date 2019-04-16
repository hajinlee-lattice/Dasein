package com.latticeengines.domain.exposed.cdl;

import java.util.Date;
import java.util.List;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.pls.Action;

@JsonIgnoreProperties(ignoreUnknown = true)
@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonAutoDetect(fieldVisibility = JsonAutoDetect.Visibility.NONE, getterVisibility = JsonAutoDetect.Visibility.NONE)
public class ActivityObject {

    @JsonProperty
    private String tenantName;

    @JsonProperty
    private Boolean scheduleNow;

    @JsonProperty
    private Date scheduleTime;

    @JsonProperty
    private List<Action> actions;

    @JsonProperty
    private Boolean dataCloudRefresh;

    @JsonProperty
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
