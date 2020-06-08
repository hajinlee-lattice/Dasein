package com.latticeengines.pls.controller;

import java.util.List;

import javax.inject.Inject;

import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "PlayTypes", description = "REST resource for play types")
@RestController
@RequestMapping("/playtypes")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class PlayTypeResource {

    @Inject
    private PlayProxy playProxy;

    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Get all playtypes for a tenant")
    public List<PlayType> getPlayTypes() {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayTypes(tenant.getId());
    }

    @PostMapping
    @ResponseBody
    @ApiOperation(value = "Create new Playtype")
    public PlayType createPlayType(@RequestBody PlayType playType) {
        Tenant tenant = MultiTenantContext.getTenant();
        String userId = MultiTenantContext.getEmailAddress();
        playType.setCreatedBy(userId);
        playType.setUpdatedBy(userId);
        return playProxy.createPlayType(tenant.getId(), playType);
    }

    @GetMapping("/{playTypeId}")
    @ResponseBody
    @ApiOperation(value = "Get a playtype for a tenant given its ID")
    public PlayType getPlayTypeById(@PathVariable String playTypeId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayTypeById(tenant.getId(), playTypeId);
    }

    @PostMapping("/{playTypeId}")
    @ResponseBody
    @ApiOperation(value = "Update a playtype given its ID")
    public void updatePlayType(@PathVariable String playTypeId, @RequestBody PlayType playType) {
        Tenant tenant = MultiTenantContext.getTenant();
        String userId = MultiTenantContext.getEmailAddress();
        playType.setUpdatedBy(userId);
        playProxy.updatePlayType(tenant.getId(), playTypeId, playType);
    }

    @DeleteMapping("/{playTypeId}")
    @ResponseBody
    @ApiOperation(value = "Delete a playtype given its ID")
    public void deletePlayType(@PathVariable String playTypeId) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.deletePlayTypeById(tenant.getId(), playTypeId);
    }
}
