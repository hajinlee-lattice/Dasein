package com.latticeengines.playmaker.controller;

import java.util.Map;

import javax.inject.Inject;

import org.springframework.http.HttpHeaders;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
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
@RequestMapping("/playmaker/tenants")
public class TenantResource {

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
    public void updateTenant(@PathVariable String tenantName, //
            @RequestBody PlaymakerTenant tenant) {
        playmakerEntityMgr.updateByTenantName(tenant);
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
    public String getOauthbearerTokenToTenant(@RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        return OAuth2Utils.getTenantName(bearerToken, oAuthUserEntityMgr);
    }

    @GetMapping("/oauthtoappid")
    @ResponseBody
    @ApiOperation(value = "Get tenant info from OAuth bearerToken")
    public Map<String, String> getAppIdFromOauthbearerToken(
            @RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        return OAuth2Utils.getAppId(bearerToken, oAuthUserEntityMgr);
    }

    @GetMapping("/oauthtoorginfo")
    @ResponseBody
    @ApiOperation(value = "Get tenant info from OAuth bearerToken")
    public Map<String, String> getOrgInfoFromOauthbearerToken(
            @RequestHeader(HttpHeaders.AUTHORIZATION) String bearerToken) {
        return OAuth2Utils.getOrgInfo(bearerToken, oAuthUserEntityMgr);
    }
}
