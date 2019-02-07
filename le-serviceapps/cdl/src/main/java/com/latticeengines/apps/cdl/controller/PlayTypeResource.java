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
import com.latticeengines.apps.cdl.entitymgr.PlayEntityMgr;
import com.latticeengines.apps.cdl.entitymgr.PlayTypeEntityMgr;
import com.latticeengines.apps.cdl.service.PlayTypeService;
import com.latticeengines.db.exposed.entitymgr.TenantEntityMgr;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.pls.PlayType;
import com.latticeengines.domain.exposed.security.Tenant;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "Play Types", description = "REST resource for play types")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/playtypes")
public class PlayTypeResource {
    private static final Logger log = LoggerFactory.getLogger(PlayTypeResource.class);

    @Inject
    private PlayTypeService playTypeService;

    @Inject
    private PlayEntityMgr playEntityMgr;

    @Inject
    private PlayTypeEntityMgr playTypeEntityMgr;

    @Inject
    private TenantEntityMgr tenantEntityMgr;

    @GetMapping(value = "", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all play types for a tenant")
    public List<PlayType> getPlayTypes(@PathVariable String customerSpace) {
        return playTypeService.getAllPlayTypes(customerSpace);
    }

    @PostMapping(value = "", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create new Play type")
    public PlayType createPlayType(@PathVariable String customerSpace, @RequestBody PlayType playType) {
        List<String> validationErrors = validatePlayTypeForCreation(playType);
        if (CollectionUtils.isEmpty(validationErrors)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
            playType.setId(PlayType.generateId());
            playType.setTenant(tenant);
            playTypeEntityMgr.create(playType);
            return playType;
        } else {
            AtomicInteger i = new AtomicInteger(1);
            throw new LedpException(LedpCode.LEDP_32000,
                    validationErrors.stream().map(err -> "\n" + i.getAndIncrement() + ". " + err).toArray());
        }
    }

    @GetMapping(value = "/{playTypeId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get a playtype given its id")
    public PlayType getPlayTypeById(@PathVariable String customerSpace, @PathVariable String playTypeId) {
        return playTypeEntityMgr.findById(playTypeId);
    }

    @PostMapping(value = "/{playTypeId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update a playtype given its id")
    public void updatePlayType(@PathVariable String customerSpace, @PathVariable String playTypeId,
            @RequestBody PlayType playType) {
        List<String> validationErrors = validatePlayTypeForUpdate(playType);
        if (CollectionUtils.isEmpty(validationErrors)) {
            Tenant tenant = tenantEntityMgr.findByTenantId(CustomerSpace.parse(customerSpace).toString());
            playType.setTenant(tenant);
            playTypeEntityMgr.update(playType);
        } else {
            AtomicInteger i = new AtomicInteger(1);
            throw new LedpException(LedpCode.LEDP_32000,
                    validationErrors.stream().map(err -> "\n" + i.getAndIncrement() + ". " + err).toArray());
        }
    }

    @DeleteMapping(value = "/{playTypeId}", headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Delete a playtype given its id")
    public void deletePlayType(@PathVariable String customerSpace, @PathVariable String playTypeId) {
        PlayType toDelete = playTypeEntityMgr.findById(playTypeId);
        if (toDelete == null) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {"No PlayType found with Id: " + playTypeId});
        }
        Long playsUsingPlayType = playEntityMgr.countByPlayTypePid(toDelete.getPid());
        if (playsUsingPlayType > 0) {
            throw new LedpException(LedpCode.LEDP_32000, new String[] {
                    "Unable to delete play type: " + toDelete.getDisplayName() + " since plays of this type exist"});
        }
        playTypeEntityMgr.delete(toDelete);
    }

    private List<String> validatePlayTypeForUpdate(PlayType playType) {
        List<String> validationErrors = validatePlayType(playType);
        if (StringUtils.isEmpty(playType.getId())) {
            validationErrors.add("Id cannot be empty");
        } else {
            PlayType existing = playTypeEntityMgr.findById(playType.getId());
            if (existing == null) {
                validationErrors.add("No Play type found for Id:" + playType.getId());

            } else if (!existing.getPid().equals(playType.getPid())) {
                log.error(MessageFormat.format(
                        "PIDs do not match for PlayType to be updated ({0}) with the persisted version ({1}), fixing it",
                        playType.getPid(), existing.getPid()));
                playType.setPid(existing.getPid());
            }
        }
        return validationErrors;
    }

    private List<String> validatePlayTypeForCreation(PlayType playType) {
        List<String> validationErrors = validatePlayType(playType);
        if (StringUtils.isNotEmpty(playType.getId()) || playType.getPid() != null) {
            if (playTypeEntityMgr.findById(playType.getId()) != null
                    || playTypeEntityMgr.findByPid(playType.getPid()) != null) {
                validationErrors.add("Play type already exists");
                log.error("Play type already exists with PID: " + playType.getPid() + " or ID:" + playType.getId());
            } else {
                validationErrors.add("ID or PID must be empty to create a new Type");
            }
        }
        return validationErrors;
    }

    private List<String> validatePlayType(PlayType playType) {
        List<String> validationErrors = new ArrayList<>();
        if (StringUtils.isEmpty(playType.getCreatedBy()) || StringUtils.isEmpty(playType.getUpdatedBy())) {
            validationErrors.add("CreatedBy and/or Updatedby cannot be empty");
        }
        if (StringUtils.isEmpty(playType.getDisplayName())) {
            validationErrors.add("Display Name cannot be empty");
        }
        return validationErrors;
    }
}
