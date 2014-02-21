package com.latticeengines.dataplatform.runtime.metric;

import org.apache.hadoop.metrics2.MetricsInfo;

public enum AnalyticJobMetricsInfo implements MetricsInfo {
    AnalyticJobMetrics("Analytic job related metrics"), //
    AppId("Application attempt id"), //
    ContainerId("Container id"), //
    Priority("Priority"), //
    Queue("Queue"), //
    AMSubmissionToRunningWaitTime("Submission to app master execution wait time"), //
    AMRunningToContainerLaunchWaitTime("Container launch wait time"), //
    NumberOfContainerPreemptions("Number of container preemptions");

    private final String description;

    AnalyticJobMetricsInfo(String description) {
        this.description = description;
    }

    @Override
    public String description() {
        return description;
    }

}
