package com.latticeengines.apps.dcp.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of service app")
@RestController
@RequestMapping("/health")
public class HealthResource {

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    @NoCustomerSpace
    public StatusDocument healthCheck() {
        return StatusDocument.online();
    }
}
