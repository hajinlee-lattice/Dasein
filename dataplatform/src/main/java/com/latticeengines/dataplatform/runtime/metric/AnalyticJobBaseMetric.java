package com.latticeengines.dataplatform.runtime.metric;

import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.AnalyticJobMetrics;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.Priority;
import static com.latticeengines.dataplatform.runtime.metric.AnalyticJobMetricsInfo.Queue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsSource;

public abstract class AnalyticJobBaseMetric implements MetricsSource {
    protected static final Log log = LogFactory.getLog(AnalyticJobBaseMetric.class);
    
    private boolean changed = false;
    private AnalyticJobMetricsInfo metric;
    private MetricsProvider metricsProvider;
    
    private static Map<String, AnalyticJobBaseMetric> map = new HashMap<String, AnalyticJobBaseMetric>();
    
    protected static void register(AnalyticJobBaseMetric metric) {
        map.put(metric.getName(), metric);
    }
    
    public static Map<String, AnalyticJobBaseMetric> getRegistry() {
        return map;
    }
    
    public AnalyticJobBaseMetric(MetricsProvider metricsProvider, AnalyticJobMetricsInfo metric) {
        this.metricsProvider = metricsProvider;
        this.metric = metric;
        register(this);
    }
    
    protected MetricsProvider getProvider() {
        return metricsProvider;
    }
    
    protected List<MetricsRecordBuilder> getMetricsBuilders(MetricsProvider provider, MetricsCollector collector, boolean all) {
        List<MetricsRecordBuilder> builderList = new ArrayList<MetricsRecordBuilder>();
        
        builderList.add(collector.addRecord(AnalyticJobMetrics) //
                .setContext("ledpjob") //
                .tag(Queue, provider.getQueue()));
        builderList.add(collector.addRecord(AnalyticJobMetrics) //
                .setContext("ledpjob") //
                .tag(Priority, provider.getPriority()));
        return builderList;
    }

    public boolean isChanged() {
        synchronized (this) {
            return changed;    
        }
    }

    public void setChanged(boolean changed) {
        synchronized (this) {
            this.changed = changed;    
        }
    }
    
    public String getName() {
        return metric.name();
    }
    
    public String getDescription() {
        return metric.description();
    }
}
