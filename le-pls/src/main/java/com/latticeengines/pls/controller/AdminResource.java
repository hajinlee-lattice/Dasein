package com.latticeengines.pls.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "admin", description = "REST resource for managing PLS tenants")
@RestController
@RequestMapping(value = "/admin")
public class AdminResource extends InternalResourceBase {
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @RequestMapping(value = "/tenants", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a PLS tenant")
    public Boolean addTenant(@RequestBody Tenant tenant, HttpServletRequest request) {
        checkHeader(request);
        tenantEntityMgr.create(tenant);
        return true;
    }

    @RequestMapping(value = "/tenants", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of registered tenants")
    public List<Tenant> getTenants(HttpServletRequest request) {
        checkHeader(request);
        return tenantEntityMgr.findAll();
    }
    
    

}
