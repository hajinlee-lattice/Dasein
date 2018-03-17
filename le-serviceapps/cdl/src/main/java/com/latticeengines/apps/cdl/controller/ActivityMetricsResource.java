package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.service.ActivityMetricsService;
import com.latticeengines.domain.exposed.metadata.transaction.ActivityType;
import com.latticeengines.domain.exposed.serviceapps.cdl.ActivityMetrics;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "metrics", description = "REST resource for activity metrics management")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/metrics")
public class ActivityMetricsResource {
    @Inject
    private ActivityMetricsService metricsService;

    @GetMapping(value = "/{type}")
    @ApiOperation(value = "Get all the active metrics for specific activity type")
    public List<ActivityMetrics> getActivityMetrics(@PathVariable String customerSpace,
            @PathVariable ActivityType type) {
        return metricsService.findActiveWithType(type);
    }

    @PostMapping(value = "/{type}")
    @ApiOperation(value = "Save purchase metrics")
    public List<ActivityMetrics> saveActivityMetrics(@PathVariable String customerSpace,
            @PathVariable ActivityType type, @RequestBody List<ActivityMetrics> metrics) {
        return metricsService.save(type, metrics);
    }
}
