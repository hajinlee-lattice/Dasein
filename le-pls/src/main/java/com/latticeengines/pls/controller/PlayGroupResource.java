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
import com.latticeengines.domain.exposed.pls.PlayGroup;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.proxy.exposed.cdl.PlayProxy;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "PlayGroups", description = "REST resource for play groups")
@RestController
@RequestMapping("/playgroups")
@PreAuthorize("hasRole('View_PLS_Plays')")
public class PlayGroupResource {

    @Inject
    private PlayProxy playProxy;

    @GetMapping(value = "", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all playgroups for a tenant")
    public List<PlayGroup> getPlayGroups() {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayGroups(tenant.getId());
    }

    @PostMapping(value = "", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create new Playgroup")
    public PlayGroup createPlayGroup(@RequestBody PlayGroup playGroup) {
        Tenant tenant = MultiTenantContext.getTenant();
        String userId = MultiTenantContext.getEmailAddress();
        playGroup.setCreatedBy(userId);
        playGroup.setUpdatedBy(userId);
        return playProxy.createPlayGroup(tenant.getId(), playGroup);
    }

    @GetMapping(value = "/{playGroupId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a playGroup for a tenant given its ID")
    public PlayGroup getPlayGroupById(@PathVariable String playGroupId) {
        Tenant tenant = MultiTenantContext.getTenant();
        return playProxy.getPlayGroupById(tenant.getId(), playGroupId);
    }

    @PostMapping(value = "/{playGroupId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a playGroup given its ID")
    public void updatePlayGroup(@PathVariable String playGroupId, @RequestBody PlayGroup playGroup) {
        Tenant tenant = MultiTenantContext.getTenant();
        String userId = MultiTenantContext.getEmailAddress();
        playGroup.setUpdatedBy(userId);
        playProxy.updatePlayGroup(tenant.getId(), playGroupId, playGroup);
    }

    @DeleteMapping(value = "/{playGroupId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a playGroup given its ID")
    public void deletePlayGroup(@PathVariable String playGroupId) {
        Tenant tenant = MultiTenantContext.getTenant();
        playProxy.deletePlayGroupById(tenant.getId(), playGroupId);
    }
}
