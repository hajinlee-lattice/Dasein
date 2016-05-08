package com.latticeengines.monitor.metric.service.impl;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Collection;

import javax.annotation.PostConstruct;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.metric.Dimension;
import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.common.exposed.util.MetricUtils;
import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.monitor.metric.MetricStoreImpl;
import com.latticeengines.monitor.metric.service.MetricWriter;

@Component("splunkLogMetricWriter")
public class SplunkLogMetricWriter implements MetricWriter {

    private static final Log log = LogFactory.getLog(SplunkLogMetricWriter.class);
    private boolean enabled = true;

    @Autowired
    private VersionManager versionManager;

    @Value("${monitor.influxdb.environment:Local}")
    private String environment;

    @Autowired
    @Qualifier("monitorExecutor")
    private ThreadPoolTaskExecutor monitorExecutor;

    private String logPrefix;

    @PostConstruct
    private void postConstruct() {
        logPrefix = String.format("%s=\"%s\" ", MetricUtils.TAG_HOST,
                getHostName() == null ? MetricUtils.NULL : getHostName());
        logPrefix += String.format("%s=\"%s\" ", MetricUtils.TAG_ENVIRONMENT, environment);
        logPrefix += String.format("%s=\"%s\" ", MetricUtils.TAG_ARTIFACT_VERSION,
                StringUtils.isEmpty(versionManager.getCurrentVersion()) ? MetricUtils.NULL
                        : versionManager.getCurrentVersion());
    }

    @Override
    public <F extends Fact, D extends Dimension> void write(MetricDB db,
            Collection<? extends Measurement<F, D>> measurements) {
        if (enabled) {
            monitorExecutor.submit(new MetricRunnable<>(db, measurements));
        }
    }

    @Override
    public void disable() {
        if (enabled) {
            log.info("Disable splunk log metric writer.");
            enabled = false;
        }
    }

    private class MetricRunnable<F extends Fact, D extends Dimension> implements Runnable {

        private MetricDB metricDb;
        private Collection<? extends Measurement<F, D>> measurements;

        MetricRunnable(MetricDB metricDb, Collection<? extends Measurement<F, D>> measurements) {
            this.metricDb = metricDb;
            this.measurements = measurements;
        }

        @Override
        public void run() {
            for (Measurement<F, D> measurement : measurements) {
                if (measurement.getMetricStores().contains(MetricStoreImpl.SPLUNK_LOG)) {
                    log.info(logPrefix + "MetricDB=\"" + metricDb + "\" " + MetricUtils.toLogMessage(measurement));
                }
            }
        }
    }

    private static String getHostName() {
        try {
            InetAddress addr;
            addr = InetAddress.getLocalHost();
            return addr.getHostName();
        } catch (UnknownHostException ex) {
            log.error("Hostname can not be resolved");
            return "unknown";
        }
    }

}
