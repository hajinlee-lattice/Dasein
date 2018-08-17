package com.latticeengines.playmaker.measurements;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class PlaymakerPlayMetrics implements Dimension, Fact {
    private Integer getPlayDurationMS;

    private Integer maximum;

    private String dataPlatform;
    private String tenantId;

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

    @MetricField(name = "GetPlayDurationMS", fieldType = MetricField.FieldType.INTEGER)
    public Integer getPlayDurationMS() {
        return getPlayDurationMS;
    }

    public void setGetPlayDurationMS(int getPlayDurationMS) {
        this.getPlayDurationMS = getPlayDurationMS;
    }

}
