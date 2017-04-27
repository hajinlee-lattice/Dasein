package com.latticeengines.matchapi.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.datacloud.core.service.RateLimitingService;
import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of match API")
@RestController
@RequestMapping("/health")
public class HealthResource {

    @Autowired
    private RateLimitingService ratelimitingService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public StatusDocument healthCheck() {
        return StatusDocument.online();
    }

    @RequestMapping(value = "/dnbstatus", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "DnB Rate Limit Status")
    public StatusDocument dnbRateLimitStatus() {
        boolean dnbAvailable = ratelimitingService.acquireDnBBulkRequest(1, true).isAllowed();
        if (dnbAvailable) {
            return StatusDocument.ok();
        } else {
            return StatusDocument.unavailable();
        }
    }
}
