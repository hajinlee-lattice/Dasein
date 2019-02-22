package com.latticeengines.pls.controller;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.hadoop.exposed.service.EMRCacheService;
import com.latticeengines.pls.service.SystemStatusService;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of LP app")
@RestController
@RequestMapping("/health")
public class HealthCheckResource {

    @Inject
    private SystemStatusService systemConfigService;

    @Inject
    private VersionManager versionManager;

    @Inject
    private EMRCacheService emrCacheService;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    @Value("${pls.current.stack:}")
    private String currentStack;

    @GetMapping(value = "")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public StatusDocument healthCheck() {
        return StatusDocument.online();
    }

    @GetMapping(value = "/systemstatus")
    @ResponseBody
    @ApiOperation(value = "System check")
    @NoMetricsLog
    public StatusDocument systemCheck() {
        StatusDocument status = systemConfigService.getSystemStatus();
        return status;
    }

    @GetMapping(value = "/stackinfo")
    @ResponseBody
    @ApiOperation(value = "Get current active stack")
    public Map<String, String> getStackInfo() {
        Map<String, String> response = new HashMap<>();
        response.put("CurrentStack", currentStack);
        response.put("ArtifactVersion", versionManager.getCurrentVersion());
        response.put("GitCommit", versionManager.getCurrentGitCommit());
        response.put("EMRClusterName", clusterName);
        response.put("EMRClusterId", emrCacheService.getClusterId(clusterName));
        return response;
    }
}
