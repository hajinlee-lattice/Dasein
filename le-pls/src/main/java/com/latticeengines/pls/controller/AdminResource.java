package com.latticeengines.pls.controller;

import java.util.List;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.security.UserRegistrationWithTenant;
import com.latticeengines.pls.service.TenantService;
import com.latticeengines.security.exposed.InternalResourceBase;
import com.latticeengines.security.exposed.service.UserService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "admin", description = "REST resource for managing PLS tenants")
@RestController
@RequestMapping(value = "/admin")
public class AdminResource extends InternalResourceBase {
    @SuppressWarnings("unused")
    private static final Log log = LogFactory.getLog(AdminResource.class);
    
    @Autowired
    private TenantService tenantService;
    
    @Autowired
    private UserService userService;

    @RequestMapping(value = "/tenants", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a PLS tenant")
    public Boolean addTenant(@RequestBody Tenant tenant, HttpServletRequest request) {
        checkHeader(request);
        if (!tenantService.hasTenantId(tenant.getId())) {
            tenantService.registerTenant(tenant);
        } else {
            tenantService.updateTenant(tenant);
        }
        return true;
    }

    @RequestMapping(value = "/tenants", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get list of registered tenants")
    public List<Tenant> getTenants(HttpServletRequest request) {
        checkHeader(request);
        return tenantService.getAllTenants();
    }

    @RequestMapping(value = "/tenants/{tenantId:.+}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a tenant.")
    public Boolean deleteTenant(@PathVariable String tenantId, HttpServletRequest request) {
        checkHeader(request);
        Tenant tenant = new Tenant();
        tenant.setName(" ");
        tenant.setId(tenantId);
        tenantService.discardTenant(tenant);
        return true;
    }
    
    @RequestMapping(value = "/users", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a PLS admin user")
    public Boolean addAdminUser(@RequestBody UserRegistrationWithTenant userRegistrationWithTenant, HttpServletRequest request) {
        checkHeader(request);
        return userService.addAdminUser(userRegistrationWithTenant);
    }
    
}
