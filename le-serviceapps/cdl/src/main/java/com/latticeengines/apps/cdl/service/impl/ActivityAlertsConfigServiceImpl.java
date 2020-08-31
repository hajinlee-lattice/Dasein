package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.apps.cdl.entitymgr.ActivityAlertsConfigEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityAlertsConfigService;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;

@Component("activityAlertsConfigService")
public class ActivityAlertsConfigServiceImpl implements ActivityAlertsConfigService {
    private static final Logger log = LoggerFactory.getLogger(ActivityAlertsConfigServiceImpl.class);

    @Inject
    private ActivityAlertsConfigEntityMgr activityAlertsConfigEntityMgr;

    @Override
    public List<ActivityAlertsConfig> findAllByTenant(String customerSpace) {
        Tenant tenant = MultiTenantContext.getTenant();
        return activityAlertsConfigEntityMgr.findAllByTenant(tenant);
    }

    @Override
    public ActivityAlertsConfig createOrUpdate(String customerSpace, ActivityAlertsConfig activityAlertsConfig) {
        // Add validation here before create/update
        if (!TemplateUtils.isValid(activityAlertsConfig.getAlertMessageTemplate()))
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Invalid alert message template, not a valid freemarker template" });

        return activityAlertsConfig;
    }

    @Override
    public void createDefaultActivityAlertsConfigs(String customerSpace) {
        // create Defaults
    }

    @Override
    public void delete(String customerSpace, ActivityAlertsConfig activityAlertsConfig) {
        ActivityAlertsConfig oldActivityAlertsConfig = activityAlertsConfigEntityMgr
                .findByPid(activityAlertsConfig.getPid());
        if (oldActivityAlertsConfig == null) {
            log.warn("Unable to find ActivityAlertsConfig (Header:{}, Id:{}) in tenant {}",
                    activityAlertsConfig.getAlertHeader(), activityAlertsConfig.getId(), customerSpace);
            return;
        }
        activityAlertsConfigEntityMgr.delete(oldActivityAlertsConfig);
    }

}
