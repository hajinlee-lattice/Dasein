package com.latticeengines.ulysses.controller;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.monitor.exposed.annotation.IgnoreGlobalApiMeter;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "generic", description = "REST resource for generic ulysses functions")
@RestController
@RequestMapping("/generic")
public class GenericResource {

    @Inject
    private Oauth2RestApiProxy tenantProxy;

    @GetMapping("/oauthtotenant")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    @IgnoreGlobalApiMeter
    public String oauthToTenant(@RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        return tenantProxy.getTenantNameFromOAuthRequest(bearerToken);
    }

    @GetMapping("/oauthtoappid")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    @IgnoreGlobalApiMeter
    public Map<String, String> oauthToAppId(@RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        return tenantProxy.getAppIdFromOAuthRequest(bearerToken);
    }
}
