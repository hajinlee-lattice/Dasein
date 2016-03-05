package com.latticeengines.pls.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.network.exposed.oauth.Oauth2Interface;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "oauth", description = "REST resource for managing PLS tenants")
@RestController
@RequestMapping(value = "/oauth")
public class Oauth2Resource {
    private static final Logger log = Logger.getLogger(Oauth2Resource.class);

    @Autowired
    private Oauth2Interface oauth2Service;

    @RequestMapping(value = "/createapitoken", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Oauth Api Token for a tenant")
    @PreAuthorize("hasRole('Create_PLS_OauthToken')")
    public String createApiToken(@RequestParam(value = "tenantId") String tenantId) {
        log.info("Generating api token for tenant " + tenantId);
        return oauth2Service.createAPIToken(tenantId);
    }

    @RequestMapping(value = "/createacesstoken", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Oauth Access and Refresh Token for a tenant")
    @PreAuthorize("hasRole('Create_PLS_OauthToken')")
    public String createOAuth2AccessToken(@RequestParam(value = "tenantId") String tenantId) {
        log.info("Generating access token and refresh token for tenant " + tenantId);
        return oauth2Service.createOAuth2AccessToken(tenantId).getValue();
    }
}
