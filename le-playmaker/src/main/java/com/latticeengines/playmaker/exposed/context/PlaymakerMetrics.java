package com.latticeengines.playmaker.exposed.context;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class PlaymakerMetrics implements Dimension, Fact {
    private Integer getRecommendationDurationMS;

    private Integer maximum;

    private String tenantId;
    private String syncDestination;
    private String destinationOrgId;
    private String destinationSysType;

    @MetricTag(tag = "TenantId")
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @MetricField(name = "Maximum", fieldType = MetricField.FieldType.INTEGER)
    public Integer getMaximum() {
        return maximum;
    }

    public void setMaximum(int maximum) {
        this.maximum = maximum;
    }

    @MetricTag(tag = "SyncDestination")
    public String getSyncDestination() {
        return syncDestination;
    }

    public void setSyncDestination(String syncDestination) {
        this.syncDestination = syncDestination;
    }

    @MetricTag(tag = "DestinationOrgId")
    public String getDestinationOrgId() {
        return destinationOrgId;
    }

    public void setDestinationOrgId(String destinationOrgId) {
        this.destinationOrgId = destinationOrgId;
    }

    @MetricTag(tag = "DestinationSysType")
    public String getDestinationSysType() {
        return destinationSysType;
    }

    public void setDestinationSysType(String destinationSysType) {
        this.destinationSysType = destinationSysType;
    }

    @MetricField(name = "GetRecommendationDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getRecommendationDurationMS() {
        return getRecommendationDurationMS;
    }

    public void setGetRecommendationDurationMS(int getRecommendationDurationMS) {
        this.getRecommendationDurationMS = getRecommendationDurationMS;
    }

}
