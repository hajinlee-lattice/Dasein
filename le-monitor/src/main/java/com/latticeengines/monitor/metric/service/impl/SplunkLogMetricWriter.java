package com.latticeengines.monitor.metric.service.impl;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.monitor.metric.MetricStoreImpl;
import com.latticeengines.monitor.metric.service.MetricWriter;

@Component("splunkLogMetricWriter")
public class SplunkLogMetricWriter implements MetricWriter {

    private static final Logger log = LoggerFactory.getLogger(SplunkLogMetricWriter.class);
    private boolean enabled = true;

    @Value("${monitor.influxdb.environment:Local}")
    private String environment;

    @Autowired
    @Qualifier("monitorExecutor")
    private ThreadPoolTaskExecutor monitorExecutor;

    private String logPrefix;

    @PostConstruct
    private void postConstruct() {
        logPrefix = String.format("%s=\"%s\" ", MetricUtils.TAG_ENVIRONMENT, environment);
    }

    @Override
    public <F extends Fact, D extends Dimension> void write(MetricDB db,
            List<? extends Measurement<F, D>> measurements, List<Map<String, Object>> fieldMaps) {
        if (enabled) {
            monitorExecutor.submit(new MetricRunnable<>(db, measurements, fieldMaps));
        }
    }

    private <F extends Fact, D extends Dimension> void writeInternal(MetricDB db,
            List<? extends Measurement<F, D>> measurements, List<Map<String, Object>> fieldMaps) {
        for (int i = 0; i < measurements.size(); i++) {
            Measurement<F, D> measurement = measurements.get(i);
            Map<String, Object> fieldMap = new HashMap<>();
            if (fieldMaps != null) {
                fieldMap = fieldMaps.get(i);
            }
            if (measurement.getMetricStores().contains(MetricStoreImpl.SPLUNK_LOG)) {
                log.debug(logPrefix + "MetricDB=\"" + db + "\" " + MetricUtils.toLogMessage(measurement, fieldMap));
            }
        }
    }

    @Override
    public void disable() {
        if (enabled) {
            log.info("Disable splunk log metric writer.");
            enabled = false;
        }
    }

    @Override
    public void enable() {
        if (!enabled) {
            log.info("Enable splunk log metric writer.");
            enabled = true;
        }
    }

    private class MetricRunnable<F extends Fact, D extends Dimension> implements Runnable {

        private MetricDB metricDb;
        private List<? extends Measurement<F, D>> measurements;
        private List<Map<String, Object>> fieldMaps;

        MetricRunnable(MetricDB metricDb, List<? extends Measurement<F, D>> measurements,
                List<Map<String, Object>> fieldMaps) {
            this.metricDb = metricDb;
            this.measurements = measurements;
            this.fieldMaps = fieldMaps;
        }

        @Override
        public void run() {
            writeInternal(metricDb, measurements, fieldMaps);
        }
    }

}
