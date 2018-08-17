package com.latticeengines.playmaker.measurements;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class PlaymakerAccExtMetrics implements Dimension, Fact {
    private Integer getAccountExtensionDurationMS;

    private Integer maximum;

    private String dataPlatform;

    private String tenantId;
    private String destinationOrgId;
    private String destinationSysType;

    @MetricTag(tag = "TenantId")
    public String getTenantId() {
        return tenantId;
    }

    public void setTenantId(String tenantId) {
        this.tenantId = tenantId;
    }

    @MetricTag(tag = "DataPlatform")
    public String getDataPlatform() {
        return dataPlatform;
    }

    public void setDataPlatform(String dataPlatform) {
        this.dataPlatform = dataPlatform;
    }

    @MetricField(name = "Maximum", fieldType = MetricField.FieldType.INTEGER)
    public Integer getMaximum() {
        return maximum;
    }

    public void setMaximum(int maximum) {
        this.maximum = maximum;
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

    @MetricField(name = "GetAccountExtensionDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getAccountExtensionDurationMS() {
        return getAccountExtensionDurationMS;
    }

    public void setGetAccountExtensionDurationMS(int getAccountExtensionDurationMS) {
        this.getAccountExtensionDurationMS = getAccountExtensionDurationMS;
    }

}
