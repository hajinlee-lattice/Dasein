package com.latticeengines.apps.cdl.controller;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.cdl.entitymgr.PlayGroupEntityMgr;
import com.latticeengines.apps.cdl.service.PlayGroupService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.PlayGroup;
import com.latticeengines.domain.exposed.security.Tenant;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Play Groups", description = "REST resource for play groups")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/playgroups")
public class PlayGroupResource {
    private static final Logger log = LoggerFactory.getLogger(PlayGroupResource.class);

    @Inject
    private PlayGroupService playGroupService;

    @Inject
    private PlayGroupEntityMgr playGroupEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @GetMapping(value = "", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all play Groups for a tenant")
    public List<PlayGroup> getPlayGroups(@PathVariable String customerSpace) {
        return playGroupService.getAllPlayGroups(customerSpace);
    }

    @PostMapping(value = "", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create new play group")
    public PlayGroup createPlayGroup(@PathVariable String customerSpace, @RequestBody PlayGroup playGroup) {
        List<String> validationErrors = validatePlayGroupForCreation(playGroup);
        if (CollectionUtils.isEmpty(validationErrors)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
            playGroup.setId(PlayGroup.generateId());
            playGroup.setTenant(tenant);
            playGroupEntityMgr.create(playGroup);
            return playGroup;
        } else {
            AtomicInteger i = new AtomicInteger(1);
            throw new LedpException(LedpCode.LEDP_32000,
                    validationErrors.stream().map(err -> "\n" + i.getAndIncrement() + ". " + err).toArray());
        }
    }

    @GetMapping(value = "/{playGroupId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a play group given its id")
    public PlayGroup getPlayGroupById(@PathVariable String customerSpace, @PathVariable String playGroupId) {
        return playGroupEntityMgr.findById(playGroupId);
    }

    @PostMapping(value = "/{playGroupId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a playgroup given its id")
    public void updatePlayGroup(@PathVariable String customerSpace, @PathVariable String playGroupId,
            @RequestBody PlayGroup playGroup) {
        List<String> validationErrors = validatePlayGroupForUpdate(playGroup);
        if (CollectionUtils.isEmpty(validationErrors)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
            playGroup.setTenant(tenant);
            playGroupEntityMgr.update(playGroup);
        } else {
            AtomicInteger i = new AtomicInteger(1);
            throw new LedpException(LedpCode.LEDP_32000,
                    validationErrors.stream().map(err -> "\n" + i.getAndIncrement() + ". " + err).toArray());
        }
    }

    @DeleteMapping(value = "/{playGroupId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a play groups given an id")
    public void deletePlayGroup(@PathVariable String customerSpace, @PathVariable String playGroupId) {
        PlayGroup toDelete = playGroupEntityMgr.findById(playGroupId);
        if (toDelete == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] { "No PlayGroup found with Id: " + playGroupId });
        }
        playGroupService.deletePlayGroupsFromPlays(customerSpace, toDelete);
    }

    private List<String> validatePlayGroupForUpdate(PlayGroup playGroup) {
        List<String> validationErrors = validatePlayGroup(playGroup);
        if (StringUtils.isEmpty(playGroup.getId())) {
            validationErrors.add("Id cannot be empty");
        } else {
            PlayGroup existing = playGroupEntityMgr.findById(playGroup.getId());
            if (existing == null) {
                validationErrors.add("No Play Group found for Id:" + playGroup.getId());

            } else if (!existing.getPid().equals(playGroup.getPid())) {
                log.error(MessageFormat.format(
                        "PIDs do not match for PlayGroup to be updated ({0}) with the persisted version ({1}), fixing it",
                        playGroup.getPid(), existing.getPid()));
                playGroup.setPid(existing.getPid());
            }
        }
        return validationErrors;
    }

    private List<String> validatePlayGroupForCreation(PlayGroup playGroup) {
        List<String> validationErrors = validatePlayGroup(playGroup);
        if (StringUtils.isNotEmpty(playGroup.getId()) || playGroup.getPid() != null) {
            if (playGroupEntityMgr.findById(playGroup.getId()) != null
                    || playGroupEntityMgr.findByPid(playGroup.getPid()) != null) {
                validationErrors.add("Play Group already exists");
                log.error("Play Group already exists with PID: " + playGroup.getPid() + " or ID:" + playGroup.getId());
            } else {
                validationErrors.add("ID or PID must be empty to create a new Group");
            }
        }
        return validationErrors;
    }

    private List<String> validatePlayGroup(PlayGroup playGroup) {
        List<String> validationErrors = new ArrayList<>();
        if (StringUtils.isEmpty(playGroup.getCreatedBy()) || StringUtils.isEmpty(playGroup.getUpdatedBy())) {
            validationErrors.add("CreatedBy and/or Updatedby cannot be empty");
        }
        if (StringUtils.isEmpty(playGroup.getDisplayName())) {
            validationErrors.add("Display Name cannot be empty");
        }
        return validationErrors;
    }
}
