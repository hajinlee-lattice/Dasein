package com.latticeengines.pls.controller;

import com.latticeengines.common.exposed.version.VersionManager;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.pls.service.SystemStatusService;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

import javax.inject.Inject;

@Api(value = "health", description = "REST resource for checking health of LP app")
@RestController
@RequestMapping("/health")
public class HealthCheckResource {

    @Inject
    private SystemStatusService systemConfigService;

    @Inject
    private VersionManager versionManager;

    @Value("${aws.emr.cluster}")
    private String clusterName;

    @Value("${pls.current.stack:}")
    private String currentStack;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public StatusDocument healthCheck() {
        return StatusDocument.online();
    }

    @RequestMapping(value = "/systemstatus", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "System check")
    @NoMetricsLog
    public StatusDocument systemCheck() {
        StatusDocument status = systemConfigService.getSystemStatus();
        return status;
    }

    @RequestMapping(value = "/stackinfo", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get current active stack")
    public Map<String, String> getStackInfo() {
        Map<String, String> response = new HashMap<>();
        response.put("CurrentStack", currentStack);
        response.put("ArtifactVersion", versionManager.getCurrentVersion());
        response.put("GitCommit", versionManager.getCurrentGitCommit());
        response.put("HadoopCluster", clusterName);
        return response;
    }
}
