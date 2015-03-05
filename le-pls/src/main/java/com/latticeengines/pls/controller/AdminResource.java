package com.latticeengines.pls.controller;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.pls.entitymanager.TenantEntityMgr;
import com.latticeengines.pls.exception.LoginException;
import com.latticeengines.pls.globalauth.authentication.impl.Constants;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "admin", description = "REST resource for managing PLS tenants")
@RestController
@RequestMapping(value = "/admin")
public class AdminResource {
    
    @Autowired
    private TenantEntityMgr tenantEntityMgr;

    @RequestMapping(value = "/tenants", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add a PLS tenant")
    public Boolean addTenant(@RequestBody Tenant tenant, HttpServletRequest request, HttpServletResponse response) {
        String value = request.getHeader(Constants.INTERNAL_SERVICE_HEADERNAME);
        
        if (value == null || !value.equals(Constants.INTERNAL_SERVICE_HEADERVALUE)) {
            throw new LoginException(new LedpException(LedpCode.LEDP_18001, new String[] {}));
        }
        tenantEntityMgr.create(tenant);
        return true;
    }

}
