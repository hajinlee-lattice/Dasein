package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.StatusDocument;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.pls.service.TenantConfigService;
import com.latticeengines.proxy.exposed.matchapi.MatchHealthProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "health", description = "REST resource for checking health of LP app")
@RestController
@RequestMapping("/health")
public class HealthCheckResource {

    @Autowired
    private TenantConfigService tenantConfigService;

    @Autowired
    private MatchHealthProxy matchHealthProxy;

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
        try {
            boolean systemStatus = tenantConfigService.getSystemStatus();
            String rateLimitStatus = matchHealthProxy.dnbRateLimitStatus().getStatus();
            if (systemStatus) {
                return StatusDocument.underMaintainance();
            } else if (rateLimitStatus.equals(StatusDocument.MATCHER_IS_BUSY)) {
                return StatusDocument.matcherIsBusy();
            }
            return StatusDocument.ok();
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_18139, "System is under maintainance" + e.getMessage(), e);
        }
    }
}