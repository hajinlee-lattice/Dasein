package com.latticeengines.ulysses.controller;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.monitor.annotation.NoMetricsLog;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "generic", description = "REST resource for generic ulysses functions")
@RestController
@RequestMapping("/generic")
public class GenericResource {

    @Inject
    private Oauth2RestApiProxy tenantProxy;

    @GetMapping(value = "/oauthtotenant", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public String oauthToTenant(RequestEntity<String> requestEntity) {
        return tenantProxy.getTenantNameFromOAuthRequest(requestEntity);
    }

    @GetMapping(value = "/oauthtoappid", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Health check")
    @NoMetricsLog
    public Map<String, String> oauthToAppId(RequestEntity<String> requestEntity) {
        return tenantProxy.getAppIdFromOAuthRequest(requestEntity);
    }
}
