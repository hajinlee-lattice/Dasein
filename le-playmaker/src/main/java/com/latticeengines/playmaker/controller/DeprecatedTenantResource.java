package com.latticeengines.playmaker.controller;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.playmaker.PlaymakerTenant;
import com.latticeengines.oauth2db.exposed.entitymgr.OAuthUserEntityMgr;
import com.latticeengines.oauth2db.exposed.entitymgr.PlaymakerTenantEntityMgr;
import com.latticeengines.oauth2db.exposed.util.OAuth2Utils;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Deprecated
@Api(value = "Playmaker tenant api", description = "REST resource for managing playmaker tenants")
@RestController
@RequestMapping("/tenants")
public class DeprecatedTenantResource {

    @Inject
    private OAuthUserEntityMgr oAuthUserEntityMgr;

    @Inject
    private PlaymakerTenantEntityMgr playmakerEntityMgr;

    @PostMapping("")
    @ResponseBody
    @ApiOperation(value = "Create a playmaker API tenant")
    public PlaymakerTenant createTenant(@RequestBody PlaymakerTenant tenant) {
        return playmakerEntityMgr.create(tenant);
    }

    @PutMapping("/{tenantName}")
    @ResponseBody
    @ApiOperation(value = "Update playmaker API tenant")
    public PlaymakerTenant updateTenant(@PathVariable String tenantName, //
            @RequestBody PlaymakerTenant tenant) {
        return playmakerEntityMgr.updateByTenantName(tenant);
    }

    @GetMapping("/{tenantName:.+}")
    @ResponseBody
    @ApiOperation(value = "Get a playmaker tenant")
    public PlaymakerTenant getTenant(@PathVariable String tenantName) {
        PlaymakerTenant tenant = playmakerEntityMgr.findByTenantName(tenantName);
        if (tenant != null) {
            return tenant;
        }
        return new PlaymakerTenant();
    }

    @DeleteMapping("/{tenantName:.+}")
    @ResponseBody
    @ApiOperation(value = "Delete playmaker tenant")
    public void deleteTenant(@PathVariable String tenantName) {
        playmakerEntityMgr.deleteByTenantName(tenantName);
    }

    @GetMapping("/oauthtotenant")
    @ResponseBody
    @ApiOperation(value = "Get tenant info from OAuth token")
    public String getOauthTokenToTenant(HttpServletRequest request) {
        return OAuth2Utils.getTenantName(request, oAuthUserEntityMgr);
    }
}
