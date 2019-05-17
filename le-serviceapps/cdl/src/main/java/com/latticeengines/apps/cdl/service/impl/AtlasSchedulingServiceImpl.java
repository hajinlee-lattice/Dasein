package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.AtlasSchedulingEntityMgr;
import com.latticeengines.apps.cdl.service.AtlasSchedulingService;
import com.latticeengines.common.exposed.util.CronUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.AtlasScheduling;

@Component("atlasSchedulingService")
public class AtlasSchedulingServiceImpl implements AtlasSchedulingService {

    private static final Logger log = LoggerFactory.getLogger(AtlasSchedulingServiceImpl.class);

    private static final String DEFAULT_CRON = "0 0 0 31 DEC ? 2099";

    @Inject
    private AtlasSchedulingEntityMgr atlasSchedulingEntityMgr;

    @Override
    public void createOrUpdateExportScheduling(String customerSpace, String cronExpression) {
        if (StringUtils.isEmpty(cronExpression)) {
            log.warn("Cron expression is empty, using default cron: " + DEFAULT_CRON);
            cronExpression = DEFAULT_CRON;
        } else if (!CronUtils.isValidExpression(cronExpression)) {
            log.error(String.format("Invalid cron expression: %s, Using default cron: %s", cronExpression,
                    DEFAULT_CRON));
            cronExpression = DEFAULT_CRON;
        }
        AtlasScheduling schedulingObj = findSchedulingByType(customerSpace, AtlasScheduling.ScheduleType.Export);
        if (schedulingObj == null) {
            schedulingObj = new AtlasScheduling();
            schedulingObj.setTenant(MultiTenantContext.getTenant());
            schedulingObj.setType(AtlasScheduling.ScheduleType.Export);
            schedulingObj.setCronExpression(cronExpression);
            atlasSchedulingEntityMgr.createSchedulingObj(schedulingObj);
        } else {
            schedulingObj.setCronExpression(cronExpression);
            atlasSchedulingEntityMgr.updateSchedulingObj(schedulingObj);
        }
    }

    @Override
    public AtlasScheduling findSchedulingByType(String customerSpace, AtlasScheduling.ScheduleType type) {
        return atlasSchedulingEntityMgr.findAtlasSchedulingByType(type);
    }

    @Override
    public List<AtlasScheduling> findAllByType(AtlasScheduling.ScheduleType type) {
        return atlasSchedulingEntityMgr.getAllAtlasSchedulingByType(type);
    }

    @Override
    public void updateExportScheduling(AtlasScheduling atlasScheduling) {
        atlasSchedulingEntityMgr.updateSchedulingObj(atlasScheduling);
    }
}
