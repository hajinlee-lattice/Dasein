package com.latticeengines.monitor.metric.service.impl;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.metric.Measurement;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.monitor.exposed.metric.service.MetricService;
import com.latticeengines.monitor.exposed.metric.service.StatsService;
import com.latticeengines.monitor.exposed.metric.stats.Inspection;

@Component("statsService")
public class StatsServiceImpl implements StatsService {

    private static final Logger log = LoggerFactory.getLogger(StatsServiceImpl.class);

    @Inject
    private MetricService metricService;

    @Resource(name = "commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @Value("${monitor.health.inspection.enabled}")
    private boolean inspectionEnabled;

    private AtomicBoolean metricSvcUnavailable = new AtomicBoolean(false);

    @Override
    public void register(Inspection inspection) {
        if (inspectionEnabled) {
            scheduler.scheduleWithFixedDelay(new InspectionRunnable(inspection), inspection.interval());
            log.info("Registered inspection " + inspection + " to scheduler " + scheduler);
        }
    }

    private class InspectionRunnable implements Runnable {

        private Inspection inspection;

        InspectionRunnable(Inspection inspection) {
            this.inspection = inspection;
        }

        @Override
        public void run() {
            List<Measurement<?, ?>> measurements = inspection.report();
            for (Measurement<?, ?> measurement : measurements) {
                try {
                    metricService.write(MetricDB.INSPECTION, measurement);
                    if (metricSvcUnavailable.get()) {
                        metricSvcUnavailable.set(false);
                    }
                } catch (Exception e) {
                    if (!metricSvcUnavailable.get()) {
                        metricSvcUnavailable.set(true);
                        log.warn("failed to write to the Inspection DB", e);
                    }
                }
            }
        }

    }

}
