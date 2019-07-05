package com.latticeengines.apps.cdl.service.impl;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.service.SchedulingPAReportService;
import com.latticeengines.domain.exposed.cdl.scheduling.report.CycleReport;
import com.latticeengines.domain.exposed.cdl.scheduling.report.PAJobReport;
import com.latticeengines.domain.exposed.cdl.scheduling.report.SchedulingReport;
import com.latticeengines.domain.exposed.cdl.scheduling.report.TenantReport;

@Lazy
@Component("schedulingPAReportService")
public class SchedulingPAReportServiceImpl implements SchedulingPAReportService {

    private static final Logger log = LoggerFactory.getLogger(SchedulingPAReportServiceImpl.class);

    @Inject
    private RedisTemplate<String, Object> redisTemplate;

    @Override
    public int saveSchedulingResult(CycleReport cycleReport,
            Map<String, List<TenantReport.SchedulingDecision>> decisions) {
        // TODO implement
        return 0;
    }

    @Override
    public int cleanupOldReports(int lastCycleId) {
        // TODO implement
        return 0;
    }

    @Override
    public Pair<Integer, Integer> getCurrentCycleIdRange() {
        // TODO implement
        return null;
    }

    @Override
    public SchedulingReport getSchedulingReport() {
        // TODO implement
        return null;
    }

    @Override
    public SchedulingReport getSchedulingReport(int startCycleId, int endCycleId) {
        // TODO implement
        return null;
    }

    @Override
    public SchedulingReport getSchedulingReport(long startScheduleTime, long endScheduleTime) {
        // TODO implement
        return null;
    }

    @Override
    public CycleReport getCycleReport(int cycleId) {
        // TODO implement
        return null;
    }

    @Override
    public TenantReport getTenantReport(String tenantId) {
        // TODO implement
        return null;
    }

    @Override
    public TenantReport getTenantReport(String tenantId, int startCycleId, int endCycleId) {
        // TODO implement
        return null;
    }

    @Override
    public TenantReport getTenantReport(String tenantId, long startScheduleTime, long endScheduleTime) {
        // TODO implement
        return null;
    }

    @Override
    public PAJobReport getPAJobReport(String applicationId) {
        // TODO implement
        return null;
    }
}
