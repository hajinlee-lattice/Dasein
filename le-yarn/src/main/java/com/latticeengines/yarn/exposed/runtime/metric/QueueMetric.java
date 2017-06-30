package com.latticeengines.yarn.exposed.runtime.metric;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;
import com.latticeengines.common.exposed.metric.annotation.MetricTag;

public class QueueMetric implements Dimension, Fact {

    private String queue;
    private Integer memoryUsed;
    private Integer vCoresUsed;
    private Integer containersUsed;
    private Integer activeApplications;
    private Integer pendingApplications;

    @MetricTag(tag = "Queue")
    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    @MetricField(name = "MemoryUsed", fieldType = MetricField.FieldType.INTEGER)
    public Integer getMemoryUsed() {
        return memoryUsed;
    }

    public void setMemoryUsed(Integer memoryUsed) {
        this.memoryUsed = memoryUsed;
    }

    @MetricField(name = "VCoresUsed", fieldType = MetricField.FieldType.INTEGER)
    public Integer getvCoresUsed() {
        return vCoresUsed;
    }

    public void setvCoresUsed(Integer vCoresUsed) {
        this.vCoresUsed = vCoresUsed;
    }

    @MetricField(name = "ContainersUsed", fieldType = MetricField.FieldType.INTEGER)
    public Integer getContainersUsed() {
        return containersUsed;
    }

    public void setContainersUsed(Integer containersUsed) {
        this.containersUsed = containersUsed;
    }

    @MetricField(name = "ActiveApplications", fieldType = MetricField.FieldType.INTEGER)
    public Integer getActiveApplications() {
        return activeApplications;
    }

    public void setActiveApplications(Integer activeApplications) {
        this.activeApplications = activeApplications;
    }

    @MetricField(name = "PendingApplications", fieldType = MetricField.FieldType.INTEGER)
    public Integer getPendingApplications() {
        return pendingApplications;
    }

    public void setPendingApplications(Integer pendingApplications) {
        this.pendingApplications = pendingApplications;
    }
}
