package com.latticeengines.pls.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.common.collect.ImmutableMap;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.network.exposed.oauth.Oauth2Interface;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "oauth2", description = "REST resource for generating oauth2 token")
@RestController
@RequestMapping(value = "/oauth2")
public class Oauth2Resource {
    private static final Logger log = Logger.getLogger(Oauth2Resource.class);

    @Autowired
    private Oauth2Interface oauth2Service;

    @RequestMapping(value = "/apitoken", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Oauth2 Api Token for a tenant")
    @PreAuthorize("hasRole('Create_PLS_Oauth2Token')")
    public String createApiToken(@RequestParam(value = "tenantId") String tenantId) {
        log.info("Generating api token for tenant " + tenantId);
        return oauth2Service.createAPIToken(tenantId);
    }

    @RequestMapping(value = "/accesstoken", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Oauth2 Access for a tenant")
    @PreAuthorize("hasRole('Create_PLS_Oauth2Token')")
    public String createOAuth2AccessToken(@RequestParam(value = "tenantId") String tenantId,
            @RequestParam(value = "appId", required = false) String appId) {
        log.info("Generating access token for tenant " + tenantId + ", app_id '" + appId + "'");
        return oauth2Service.createOAuth2AccessToken(tenantId, appId).getValue();
    }

    @RequestMapping(value = "/accesstoken/json", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Oauth2 Access for a tenant")
    @PreAuthorize("hasRole('Create_PLS_Oauth2Token')")
    public String createJsonOAuth2AccessToken(@RequestParam(value = "tenantId") String tenantId,
            @RequestParam(value = "appId", required = false) String appId) {
        log.info("Generating access token for tenant " + tenantId + ", app_id '" + appId + "'");
        String token = oauth2Service.createOAuth2AccessToken(tenantId, appId).getValue();
        return JsonUtils.serialize(ImmutableMap.<String, String> of("token", token));
    }
}
