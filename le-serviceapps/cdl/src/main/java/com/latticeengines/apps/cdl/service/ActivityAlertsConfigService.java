package com.latticeengines.apps.cdl.service;

import java.util.List;

import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;

public interface ActivityAlertsConfigService {
    List<ActivityAlertsConfig> findAllByTenant(String customerSpace);

    ActivityAlertsConfig createOrUpdate(String customerSpace, ActivityAlertsConfig ActivityAlertsConfig);

    void createDefaultActivityAlertsConfigs(String customerSpace);

    void delete(String customerSpace, ActivityAlertsConfig ActivityAlertsConfig);
}
