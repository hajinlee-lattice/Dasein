package com.latticeengines.pls.controller;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;
import com.latticeengines.pls.service.OauthService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "oauth", description = "REST resource for managing PLS tenants")
@RestController
@RequestMapping(value = "/oauth")
public class OauthResource {
    private static final Logger log = Logger.getLogger(OauthResource.class);

    @Autowired
    private OauthService oauthService;

    @RequestMapping(value = "/generateApiToken", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Generate an Api Token for a tenant")
    public String generateApiToken(@RequestParam(value = "tenantId") String tenantId) {
        log.info("Generating api token for tenant " + tenantId);
        return oauthService.generateAPIToken(tenantId);
    }
}
