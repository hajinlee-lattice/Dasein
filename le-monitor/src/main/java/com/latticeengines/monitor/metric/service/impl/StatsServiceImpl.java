package com.latticeengines.monitor.metric.service.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
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

    @Autowired
    private MetricService metricService;

    @Autowired
    @Qualifier("commonTaskScheduler")
    private ThreadPoolTaskScheduler scheduler;

    @Value("${monitor.health.inspection.enabled}")
    private boolean inspectionEnabled;

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
                } catch (Exception e) {
                    // ignore
                }
            }
        }

    }

}
