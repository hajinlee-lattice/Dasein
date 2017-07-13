package com.latticeengines.yarn.exposed.runtime.metric;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsSource;
import org.apache.hadoop.metrics2.MetricsSystem;
import org.apache.hadoop.metrics2.annotation.Metric;
import org.apache.hadoop.metrics2.annotation.Metrics;
import org.apache.hadoop.metrics2.lib.MetricsRegistry;
import org.apache.hadoop.metrics2.lib.MutableCounterInt;
import org.apache.hadoop.metrics2.lib.MutableGaugeLong;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;

@Metrics(context="ledpjob")
public class LedpMetrics implements MetricsSource {
    private static final Logger log = LoggerFactory.getLogger(LedpMetrics.class);
    
    @Metric("Container wait time") MutableGaugeLong containerWaitTime;
    @Metric("Application wait time") MutableGaugeLong applicationWaitTime;
    @Metric("Container elapsed time") MutableGaugeLong containerElapsedTime;
    @Metric("Application cleanup time") MutableGaugeLong applicationCleanupTime;
    @Metric("Number container preemptions") MutableCounterInt numContainerPreemptions;

    private final MetricsRegistry registry;
    private final MetricsSystem ms;
    private Map<MetricsInfo, String> tagsWithValues = new HashMap<MetricsInfo, String>();

    private String queue;
    private String priority;
	private String customer;
    
    protected LedpMetrics(MetricsSystem ms, List<MetricsInfo> tags) {
        this.registry = new MetricsRegistry(LedpMetricsInfo.AnalyticJobMetrics);
        this.ms = ms;
        for (MetricsInfo tag : tags) {
            tagsWithValues.put(tag, null);
        }
        ms.register(tags.get(0).name(), tags.get(0).description(), this);
    }
    
    public static LedpMetrics getForTags(MetricsSystem ms, List<MetricsInfo> tags) {
        return new LedpMetrics(ms, tags);
    }
    
    public void buildTags() {
        for (Map.Entry<MetricsInfo, String> entry : tagsWithValues.entrySet()) {
            MetricsInfo key = entry.getKey();
            String value = entry.getValue();
            
            if (value == null) {
                throw new LedpException(LedpCode.LEDP_13000, new String[] { key.name() });
            }
            registry.tag(key, value);
        }
    }
    
    public void start() {
        ms.init("ledpjob");
    }
    
    @Override
    public void getMetrics(MetricsCollector collector, boolean all) {
        if (log.isDebugEnabled()) {
            log.info("Taking metric snapshot for instance " + toString());    
        }
        
        registry.snapshot(collector.addRecord(registry.info()), false);
    }
    
    public void setTagValue(MetricsInfo tagName, String tagValue) {
        if (tagsWithValues.containsKey(tagName)) {
            tagsWithValues.put(tagName, tagValue);
        }
    }
    
    public void setApplicationCleanupTime(long appCleanupTime) {
        log.info("Set app cleanup time = " + appCleanupTime);
        applicationCleanupTime.set(appCleanupTime);
    }

    public void setApplicationWaitTime(long appWaitTime) {
        log.info("Set app wait time = " + appWaitTime);
        applicationWaitTime.set(appWaitTime);
    }
    
    public void setContainerWaitTime(long ctrWaitTime) {
        log.info("Set container wait time = " + ctrWaitTime);
        containerWaitTime.set(ctrWaitTime);
    }
    
    public void incrementNumContainerPreemptions() {
        numContainerPreemptions.incr();
    }
    
    public void setContainerElapsedTime(long ctrElapsedTime) {
        log.info("Set container elapsed time = " + ctrElapsedTime);
        containerElapsedTime.set(ctrElapsedTime);
    }

    public String getQueue() {
        return queue;
    }

    public void setQueue(String queue) {
        this.queue = queue;
    }

    public String getPriority() {
        return priority;
    }

    public void setPriority(String priority) {
        this.priority = priority;
    }
    
	public String getCustomer() {
		return customer;
	}

	public void setCustomer(String customer) {
		this.customer = customer;
	}

    public void publishMetrics() {
        ms.publishMetricsNow();
    }
}
