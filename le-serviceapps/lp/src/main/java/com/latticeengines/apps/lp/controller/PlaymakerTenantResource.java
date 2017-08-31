package com.latticeengines.apps.lp.controller;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.annotation.NoCustomerSpace;
import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.entitymgr.PlaymakerTenantEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Playmaker tenant api", description = "REST resource for managing playmaker tenants")
@RestController
@RequestMapping(value = "/playmaker/tenants")
public class PlaymakerTenantResource {

    @Autowired
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Autowired
    private PlaymakerTenantEntityMgr playmakerEntityMgr;

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create a playmaker API tenant")
    @NoCustomerSpace
    public PlaymakerTenant createTenant(@RequestBody PlaymakerTenant tenant) {
        return playmakerEntityMgr.create(tenant);
    }

    @RequestMapping(value = "/{tenantName}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update playmaker API tenant")
    @NoCustomerSpace
    public void updateTenant(@PathVariable String tenantName, @RequestBody PlaymakerTenant tenant) {
        playmakerEntityMgr.updateByTenantName(tenant);
    }

    @RequestMapping(value = "/{tenantName:.+}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a playmaker tenant")
    @NoCustomerSpace
    public PlaymakerTenant getTenant(@PathVariable String tenantName) {
        PlaymakerTenant tenant = playmakerEntityMgr.findByTenantName(tenantName);
        if (tenant != null) {
            return tenant;
        }
        return new PlaymakerTenant();
    }

    @RequestMapping(value = "/{tenantName:.+}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete playmaker tenant")
    @NoCustomerSpace
    public void deleteTenant(@PathVariable String tenantName) {
        playmakerEntityMgr.deleteByTenantName(tenantName);
    }

    @RequestMapping(value = "/oauthtotenant", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get tenant info from OAuth token")
    @NoCustomerSpace
    public String getOauthTokenToTenant(HttpServletRequest request) {
        return OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
    }
}
