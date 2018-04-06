package com.latticeengines.apps.cdl.controller;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.apps.core.service.ActionService;
import com.latticeengines.domain.exposed.pls.Action;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "actions", description = "REST resource for actions")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/actions")
public class ActionResource {

    @Inject
    private ActionService actionService;

    @GetMapping(value = "")
    @ApiOperation(value = "Get all actions")
    public List<Action> findAll(@PathVariable String customerSpace, //
            @RequestParam(name = "pids", required = false) List<Long> pids, //
            @RequestParam(name = "ownerId", required = false) Long ownerId,
            @RequestParam(name = "nullOwnerId", required = false, defaultValue = "0") boolean nullOwnerId) {
        List<Action> actions;
        if (CollectionUtils.isNotEmpty(pids)) {
            actions = actionService.findByPidIn(pids);
        } else if (nullOwnerId) {
            actions = actionService.findByOwnerId(null);
        } else if (ownerId != null) {
            actions = actionService.findByOwnerId(ownerId);
        } else {
            actions = actionService.findAll();
        }
        return actions;
    }

    @PostMapping(value = "")
    @ApiOperation(value = "Save purchase metrics")
    public Action create(@PathVariable String customerSpace, @RequestBody Action action) {
        return actionService.create(action);
    }

    @PutMapping(value = "")
    @ApiOperation(value = "Save purchase metrics")
    public Action update(@PathVariable String customerSpace, @RequestBody Action action) {
        return actionService.update(action);
    }

    @PutMapping(value = "/ownerid")
    @ApiOperation(value = "Save purchase metrics")
    public void patchOwnerId(@PathVariable String customerSpace, //
            @RequestParam(name = "pids", required = false) List<Long> pids, //
            @RequestParam(name = "ownerId", required = false) Long ownerId) {
        actionService.patchOwnerIdByPids(ownerId, pids);
    }

}
