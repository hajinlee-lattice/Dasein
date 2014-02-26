package com.latticeengines.dataplatform.runtime.metric;

import org.apache.hadoop.metrics2.MetricsInfo;

public enum LedpMetricsInfo implements MetricsInfo {
    AnalyticJobMetrics("Analytic job related metrics"), //
    AppId("Application attempt id"), //
    ContainerId("Container id"), //
    Priority("Priority"), //
    Queue("Queue");

    private final String description;

    LedpMetricsInfo(String description) {
        this.description = description;
    }

    @Override
    public String description() {
        return description;
    }

}
