package com.latticeengines.ulysses.controller;

import javax.inject.Inject;

import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.domain.exposed.ulysses.GatewayPolicyConfiguration;
import com.latticeengines.proxy.exposed.oauth2.Oauth2RestApiProxy;

//@Api(value = "api-gateway", description = "REST resource for API Gateway and Policy Config") //We do not want to publish this API via swagger.
@RestController
@RequestMapping("/api-gateway")
@ResponseBody
public class ApiGatewayResource {

    @Inject
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @GetMapping("/tenant-config")
    public GatewayPolicyConfiguration getGatewayPolicyConfiguration(
            @RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        String tenantName = oauth2RestApiProxy.getTenantNameFromOAuthRequest(bearerToken);

        if (StringUtils.isBlank(tenantName)) {
            throw new LedpException(LedpCode.LEDP_39002);
        }

        PlaymakerTenant oAuthTenant = oauth2RestApiProxy.getTenant(tenantName);

        GatewayPolicyConfiguration gwPolicyConfig = new GatewayPolicyConfiguration();
        gwPolicyConfig.setPrincipal(tenantName);
        gwPolicyConfig.setApiKey(oAuthTenant.getGwApiKey());

        return gwPolicyConfig;
    }

}
