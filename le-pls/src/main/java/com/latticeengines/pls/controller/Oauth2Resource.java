package com.latticeengines.pls.controller;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.oauth.OauthClientType;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.latticeengines.db.exposed.util.MultiTenantContext;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "oauth2", description = "REST resource for generating oauth2 token")
@RestController
@RequestMapping(value = "/oauth2")
public class Oauth2Resource {
    private static final Logger log = LoggerFactory.getLogger(Oauth2Resource.class);

    @Autowired
    private Oauth2Interface oauth2Service;

    @RequestMapping(value = "/apitoken", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Oauth2 Api Token for a tenant")
    @PreAuthorize("hasRole('Create_PLS_Oauth2Token')")
    public String createApiToken(@RequestParam(value = "tenantId") String tenantId) {
        String targetTenantId = getTargetTenantId(tenantId);
        log.info("Generating api token for tenant " + targetTenantId);
        return oauth2Service.createAPIToken(targetTenantId);
    }

    @RequestMapping(value = "/accesstoken", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Oauth2 Access for a tenant")
    @PreAuthorize("hasRole('Create_PLS_Oauth2Token_External')")
    public String createOAuth2AccessToken(@RequestParam(value = "tenantId") String tenantId,
            @RequestParam(value = "app_id", required = false) String appId,
            @RequestParam(value = "clientType", required = false) String clientType) {
        OauthClientType client = OauthClientType.LP;
        if (StringUtils.isNotBlank(clientType)) {
            try {
                client = OauthClientType.valueOf(clientType);
            } catch (Exception e) {
                log.error("Unable to parse OAuthClientType for " + clientType, e);
                log.info("Using the default OAuthClientType " + OauthClientType.LP.getValue());
            }
        }
        String targetTenantId = getTargetTenantId(tenantId);
        log.info("Generating access token for tenant " + targetTenantId + ", app_id '" + appId + "', clientType "
                + client.getValue());
        return oauth2Service.createOAuth2AccessToken(targetTenantId, appId, client).getValue();
    }

    @RequestMapping(value = "/accesstoken/json", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Oauth2 Access token for a tenant")
    @PreAuthorize("hasRole('Create_PLS_Oauth2Token')")
    public String createJsonOAuth2AccessToken(@RequestParam(value = "tenantId") String tenantId,
            @RequestParam(value = "app_id", required = false) String appId) {
        String targetTenantId = getTargetTenantId(tenantId);
        log.info("Generating access token for tenant " + targetTenantId + ", app_id '" + appId + "'");
        String token = oauth2Service.createOAuth2AccessToken(targetTenantId, appId).getValue();
        return JsonUtils.serialize(ImmutableMap.of("token", token));
    }

    private String getTargetTenantId(String tenantIdInUrl) {
        String tenantIdInContext = MultiTenantContext.getTenant().getId();
        if (StringUtils.isBlank(tenantIdInUrl)) {
            return tenantIdInContext;
        } else {
            if (!CustomerSpace.parse(tenantIdInUrl).toString().equals(tenantIdInContext)) {
                log.warn(String.format("Cross tenant token generation: logged in %s, looking for %s", tenantIdInContext,
                        tenantIdInUrl));
            }
            return tenantIdInUrl;
        }
    }
}
