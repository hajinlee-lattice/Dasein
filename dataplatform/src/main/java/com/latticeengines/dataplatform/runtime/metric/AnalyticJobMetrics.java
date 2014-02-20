package com.latticeengines.dataplatform.runtime.metric;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AMRunningToContainerLaunchWaitTime;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AnalyticJobMetrics;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AppId;

import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;

public class AnalyticJobMetrics implements MetricsSource {
    private String appId;
    private long ts;
    private static AnalyticJobMetrics instance;

    private AnalyticJobMetrics(String appId, long ts) {
        this.appId = appId;
        this.ts = ts;
        MetricsSystem ms = DefaultMetricsSystem.instance();
        
        ms.register(AnalyticJobMetrics.name(),
                AnalyticJobMetrics.description(), this);
        ms.init("ledpjob");
        ms.publishMetricsNow();
    }

    public static AnalyticJobMetrics getInstance(String appId, long ts) {
        if (instance == null) {
            synchronized (AnalyticJobMetrics.class) {
                if (instance == null) {
                    instance = new AnalyticJobMetrics(appId, ts);
                }
            }
        }

        return instance;
    }

    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        MetricsRecordBuilder rb = collector.addRecord(AnalyticJobMetrics)
                .setContext("ledpjob").tag(AppId, appId);
        rb.addGauge(AMRunningToContainerLaunchWaitTime, ts);
    }

}
