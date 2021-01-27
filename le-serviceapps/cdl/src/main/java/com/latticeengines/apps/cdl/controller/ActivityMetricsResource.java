package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Objects;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ActivityMetricsGroupService;
import com.latticeengines.apps.cdl.service.ActivityMetricsService;
import com.latticeengines.apps.cdl.util.ActionContext;
import com.latticeengines.domain.exposed.cdl.activity.ActivityMetricsGroup;
import com.latticeengines.domain.exposed.cdl.activity.CreateActivityMetricsGroupRequest;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.pls.Action;
import com.latticeengines.domain.exposed.pls.ActivityMetricsWithAction;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metrics", description = "REST resource for activity metrics management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/metrics")
public class ActivityMetricsResource {
    @Inject
    private ActivityMetricsService metricsService;

    @Inject
    private ActivityMetricsGroupService activityMetricsGroupService;

    // For P&A profiling purchase history
    @GetMapping("/{type}")
    @ApiOperation(value = "Get all the metrics for specific activity type")
    public List<ActivityMetrics> getActivityMetrics(@PathVariable String customerSpace,
            @PathVariable ActivityType type) {
        return metricsService.findWithType(type);
    }

    // For metrics configuration
    @GetMapping("/{type}/active")
    @ApiOperation(value = "Get all the active metrics for specific activity type")
    public List<ActivityMetrics> getActiveActivityMetrics(@PathVariable String customerSpace,
            @PathVariable ActivityType type) {
        return metricsService.findActiveWithType(type);
    }

    @PostMapping("/{type}")
    @ApiOperation(value = "Save purchase metrics")
    public ActivityMetricsWithAction saveActivityMetrics(@PathVariable String customerSpace,
            @PathVariable ActivityType type, @RequestBody List<ActivityMetrics> metrics) {
        List<ActivityMetrics> saved = metricsService.save(type, metrics);
        Action action = ActionContext.getAction();
        return new ActivityMetricsWithAction(saved, action);
    }

    @PostMapping("/groups/default/webvisit")
    @ApiOperation(value = "Setup default web visit metric groups for total visit and source medium")
    public Boolean setupDefaultWebVisitGroups(@PathVariable String customerSpace,
            @RequestBody String streamName) {
        List<ActivityMetricsGroup> defaultGroups = activityMetricsGroupService.setupDefaultWebVisitGroups(customerSpace, streamName);
        if (defaultGroups == null || defaultGroups.stream().anyMatch(Objects::isNull)) {
            throw new IllegalStateException(String.format("Failed to setup default web visit metric groups for tenant %s", customerSpace));
        }
        return true;
    }

    @PostMapping("/groups/default/opportunity")
    @ApiOperation(value = "Setup default opportunity metric groups for opportunity by stage")
    public Boolean setupDefaultOpportunityGroups(@PathVariable String customerSpace, @RequestBody String streamName) {
        ActivityMetricsGroup defaultGroup = activityMetricsGroupService.setUpDefaultOpportunityGroup(customerSpace,
                streamName);
        if (defaultGroup == null) {
            throw new IllegalStateException(String.format("Failed to setup default Opportunity metric groups for tenant %s",
                    customerSpace));
        }
        return true;
    }

    @PostMapping("/groups/default/marketing")
    @ApiOperation(value = "Setup default marketing metric groups")
    public Boolean setupDefaultMarketingGroups(@PathVariable String customerSpace, @RequestBody String streamName) {
        List<ActivityMetricsGroup> defaultGroups = activityMetricsGroupService.setupDefaultMarketingGroups(customerSpace,
                streamName);
        if (defaultGroups == null) {
            throw new IllegalStateException(String.format("Failed to setup default marketing metric groups for tenant %s",
                    customerSpace));
        }
        return true;
    }

    @PostMapping("/groups/default/intent")
    @ApiOperation(value = "Setup default intent metric groups")
    public Boolean setupDefaultIntentGroups(@PathVariable String customerSpace, @RequestBody String streamName) {
        List<ActivityMetricsGroup> defaultGroups = activityMetricsGroupService.setupDefaultDnbIntentGroups(customerSpace,
                streamName);
        if (defaultGroups == null) {
            throw new IllegalStateException(String.format("Failed to setup default intent with buying score metric groups for tenant %s",
                    customerSpace));
        }
        return true;
    }


    @PostMapping("/groups/customize/create")
    @ApiOperation(value = "Setup customized metrics group")
    public Boolean createGroup(@PathVariable String customerSpace, @RequestBody CreateActivityMetricsGroupRequest request) {
        return activityMetricsGroupService.createCustomizedGroup(customerSpace, request);
    }
}
