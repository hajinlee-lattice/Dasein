package com.latticeengines.ulysses.controller;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.RequestEntity;
import org.springframework.web.bind.annotation.GetMapping;
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

    @Autowired
    private Oauth2RestApiProxy oauth2RestApiProxy;

    @GetMapping(value = "/tenant-config", headers = "Accept=application/json")
    public GatewayPolicyConfiguration getGatewayPolicyConfiguration(RequestEntity<String> requestEntity) {
        String tenantName = oauth2RestApiProxy.getTenantNameFromOAuthRequest(requestEntity);

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
