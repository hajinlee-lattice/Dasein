package com.latticeengines.apps.cdl.service.impl;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.List;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.util.StreamUtils;

import com.latticeengines.apps.cdl.entitymgr.ActivityAlertsConfigEntityMgr;
import com.latticeengines.apps.cdl.service.ActivityAlertsConfigService;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.util.TemplateUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
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
        // TODO: Add validation here before create/update
        if (!TemplateUtils.isValid(activityAlertsConfig.getAlertMessageTemplate()))
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Invalid alert message template, not a valid freemarker template" });
        createOrUpdate(activityAlertsConfig);
        return activityAlertsConfig;
    }

    @Override
    public List<ActivityAlertsConfig> createDefaultActivityAlertsConfigs(String customerSpace) {
        List<ActivityAlertsConfig> existingAlerts = findAllByTenant(customerSpace);

        if (CollectionUtils.isNotEmpty(existingAlerts)) {
            throw new LedpException(LedpCode.LEDP_32000,
                    new String[] { "Cannot create default alerts configuration since alerts already exist for tenant="
                            + CustomerSpace.shortenCustomerSpace(customerSpace) });
        }

        String defaultActivityAlertsConfigurationFilePath = "com/latticeengines/cdl/activityalerts/default-activity-alerts-configuration.json";
        try {
            ClassLoader classLoader = getClass().getClassLoader();
            InputStream configStream = classLoader.getResourceAsStream(defaultActivityAlertsConfigurationFilePath);
            String configDoc = StreamUtils.copyToString(configStream, Charset.defaultCharset());
            List<?> raw = JsonUtils.deserialize(configDoc, List.class);
            List<ActivityAlertsConfig> defaultAlerts = JsonUtils.convertList(raw, ActivityAlertsConfig.class);
            return defaultAlerts.stream().map(this::createOrUpdate).collect(Collectors.toList());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_10011, e,
                    new String[] { defaultActivityAlertsConfigurationFilePath });
        }
    }

    public ActivityAlertsConfig createOrUpdate(ActivityAlertsConfig activityAlertsConfig) {
        ActivityAlertsConfig existingAlertConfig = null;
        Tenant tenant = MultiTenantContext.getTenant();
        if (activityAlertsConfig.getPid() != null) {
            existingAlertConfig = activityAlertsConfigEntityMgr.findByPid(activityAlertsConfig.getPid());
        }
        if (existingAlertConfig == null) {
            existingAlertConfig = activityAlertsConfig;
        } else {
            existingAlertConfig.setName(activityAlertsConfig.getName());
            existingAlertConfig.setAlertCategory(activityAlertsConfig.getAlertCategory());
            existingAlertConfig.setAlertHeader(activityAlertsConfig.getAlertHeader());
            existingAlertConfig.setAlertMessageTemplate(activityAlertsConfig.getAlertMessageTemplate());
            existingAlertConfig.setQualificationPeriodDays(activityAlertsConfig.getQualificationPeriodDays());
        }
        existingAlertConfig.setTenant(tenant);
        existingAlertConfig.setActive(true);
        activityAlertsConfigEntityMgr.createOrUpdate(existingAlertConfig);
        return activityAlertsConfig;
    }

    @Override
    public void delete(String customerSpace, ActivityAlertsConfig activityAlertsConfig) {
        ActivityAlertsConfig oldActivityAlertsConfig = activityAlertsConfigEntityMgr
                .findByPid(activityAlertsConfig.getPid());
        if (oldActivityAlertsConfig == null) {
            log.warn("Unable to find ActivityAlertsConfig (Header:{}, Name:{}) in tenant {}",
                    activityAlertsConfig.getAlertHeader(), activityAlertsConfig.getName(), customerSpace);
            return;
        }
        activityAlertsConfigEntityMgr.delete(oldActivityAlertsConfig);
    }

}
