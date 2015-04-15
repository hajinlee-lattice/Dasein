package com.latticeengines.security.controller;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.security.exposed.Constants;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "globaladmintenant", description = "REST resource for Global Admin Tenant")
@RestController
@RequestMapping(value = "/globaladmintenant")
public class GlobalAdminTenantResource {

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get the id and name of global admin tenant")
    public Tenant get() {
        Tenant tenant = new Tenant();
        tenant.setId(Constants.GLOBAL_ADMIN_TENANT_ID);
        tenant.setName(Constants.GLOBAL_ADMIN_TENANT_NAME);
        return tenant;
    }
}