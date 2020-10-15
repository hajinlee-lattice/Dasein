package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ActivityAlertsConfigService;
import com.latticeengines.domain.exposed.cdl.activity.ActivityAlertsConfig;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "ActivityAlertsConfig")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/activity-alerts-config")
public class    ActivityAlertsConfigurationResource {

    @Inject
    private ActivityAlertsConfigService activityAlertsConfigService;

    @GetMapping("")
    @ApiOperation(value = "Get configuration for all Activity Alerts by tenant")
    public List<ActivityAlertsConfig> getActivityAlertsConfigurations(@PathVariable String customerSpace) {
        return activityAlertsConfigService.findAllByTenant(customerSpace);
    }

    @PutMapping("/defaults")
    @ApiOperation(value = "Generate default Activity Alert configurations")
    public List<ActivityAlertsConfig> createDefaultActivityAlertsConfigurations(@PathVariable String customerSpace) {
        return activityAlertsConfigService.createDefaultActivityAlertsConfigs(customerSpace);
    }
}
