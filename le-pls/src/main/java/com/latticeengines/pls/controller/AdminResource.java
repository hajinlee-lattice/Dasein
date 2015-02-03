package com.latticeengines.pls.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.globalauth.authentication.GlobalUserManagementService;
import com.latticeengines.pls.service.TenantAdminService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "admin", description = "REST resource for managing PLS tenants")
@RestController
@RequestMapping(value = "/admin")
public class AdminResource {
    
    @Autowired
    private TenantAdminService tenantAdminService;

    @Autowired
    private GlobalUserManagementService globalUserManagementService;

    @RequestMapping(value = "/tenants/add", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a PLS tenant")
    public String addTenant(@RequestBody Tenant tenant) {
        return " { \"value\": \"Done\" }";
    }

    @RequestMapping(value = "/tenants/{tenantName}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant for specified name")
    public Tenant getTenant(@PathVariable String tenantName) {
        Tenant tenant = new Tenant();
        tenant.setName("abc");
        return tenant;
    }

}
