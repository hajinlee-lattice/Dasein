package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.web.bind.annotation.DeleteMapping;
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

    @PostMapping(value = "/find")
    @ApiOperation(value = "Get all actions")
    @SuppressWarnings("unchecked")
    public List<Action> findAll(@PathVariable String customerSpace, @RequestBody Map<String, Object> actionParameter) {
        List<Long> pids = actionParameter.get("pids") == null ? null :
                ((List<Integer>) actionParameter.get("pids")).stream()
                        .mapToLong(Integer::longValue).boxed().collect(Collectors.toList());
        Long ownerId = actionParameter.get("ownerId") == null ? null :
                Long.valueOf(actionParameter.get("ownerId").toString());
        boolean nullOwnerId = (boolean) actionParameter.get("nullOwnerId");
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
    @ApiOperation(value = "Create Action")
    public Action create(@PathVariable String customerSpace, @RequestBody Action action) {
        return actionService.create(action);
    }

    @PutMapping(value = "")
    @ApiOperation(value = "Update Action")
    public Action update(@PathVariable String customerSpace, @RequestBody Action action) {
        return actionService.update(action);
    }

    @GetMapping(value = "")
    @ApiOperation(value = "Cancel Action")
    public Action cancel(@PathVariable String customerSpace, @RequestParam(name = "actionPid") Long actionPid) {
        return actionService.cancel(actionPid);
    }

    @DeleteMapping(value = "")
    @ApiOperation(value = "Delete Action")
    public void delete(@PathVariable String customerSpace, @RequestParam(name = "actionPid") Long actionPid) {
        actionService.delete(actionPid);
    }

    @PutMapping(value = "/ownerid")
    @ApiOperation(value = "Patch Owner Id")
    public void patchOwnerId(@PathVariable String customerSpace, //
                             @RequestParam(name = "pids", required = false) List<Long> pids, //
                             @RequestParam(name = "ownerId", required = false) Long ownerId) {
        actionService.patchOwnerIdByPids(ownerId, pids);
    }

    @PostMapping(value = "/actions")
    @ApiOperation(value = "get Actions By TrackingPids")
    public List<Action> getActionByjobPids(@PathVariable String customerSpace, @RequestBody List<Long> jobPids) {
        return actionService.getActionsByJobPids(jobPids);
    }

}
