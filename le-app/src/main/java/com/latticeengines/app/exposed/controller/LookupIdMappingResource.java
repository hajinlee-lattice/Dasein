package com.latticeengines.app.exposed.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.exception.UIActionException;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.pls.frontend.Status;
import com.latticeengines.domain.exposed.pls.frontend.UIAction;
import com.latticeengines.domain.exposed.pls.frontend.View;
import com.latticeengines.domain.exposed.remote.tray.TraySettings;
import com.latticeengines.domain.exposed.util.UIActionUtils;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "lookup-id-mapping", description = "Rest resource for lookup Id mapping")
@RestController
@RequestMapping("/lookup-id-mapping")
public class LookupIdMappingResource {

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    private static final Logger log = LoggerFactory.getLogger(LookupIdMappingResource.class);

    @GetMapping("")
    @ResponseBody
    @ApiOperation(value = "Get mapped configirations of org id and corresponding lookup id per external system type")
    public Map<String, List<LookupIdMap>> getLookupIdsMapping(HttpServletRequest request, //
            @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = false) //
            CDLExternalSystemType externalSystemType, //
            @ApiParam(value = "Sort by", required = false) //
            @RequestParam(value = "sortby", required = false) String sortby, //
            @ApiParam(value = "Sort in descending order", required = false, defaultValue = "true") //
            @RequestParam(value = "descending", required = false, defaultValue = "true") boolean descending) {
        return lookupIdMappingProxy.getLookupIdsMapping(MultiTenantContext.getTenant().getId(), externalSystemType,
                sortby, descending);
    }

    @PostMapping("/register")
    @ResponseBody
    @ApiOperation(value = "Register an org")
    public LookupIdMap registerExternalSystem(HttpServletRequest request, @RequestBody LookupIdMap lookupIdMap) {
        try {
            return lookupIdMappingProxy.registerExternalSystem(MultiTenantContext.getTenant().getId(), lookupIdMap);
        } catch (LedpException e) {
            String title = "Cannot create new connection";
            String message;

            switch (e.getCode()) {
            case LEDP_40071:
                log.error("Failed to create connection because of duplicate credentials", e);
                message = "A connection with the credentials you entered already exists. Enter different credentials.";
                break;

            case LEDP_40080:
                log.error("Failed to create connection because empty org name", e);
                message = "System name cannot be empty";
                break;

            case LEDP_40081:
                log.error("Failed to create connection because of duplicate org name", e);
                message = "A connection with the same system name already exists";
                break;

            default:
                message = e.getMessage();
                break;
            }
            UIAction action = UIActionUtils.generateUIAction(title, View.Banner, Status.Error, message);

            throw new UIActionException(action, e.getCode());
        }
    }

    @PutMapping("/deregister")
    @ResponseBody
    @ApiOperation(value = "Register an org")
    public void deregisterExternalSystem(HttpServletRequest request, @RequestBody LookupIdMap lookupIdMap) {
        lookupIdMappingProxy.deregisterExternalSystem(MultiTenantContext.getTenant().getId(), lookupIdMap);
    }

    @GetMapping("/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Get mapped configuration for given config id")
    public LookupIdMap getLookupIdMap(HttpServletRequest request, @PathVariable String id) {
        return lookupIdMappingProxy.getLookupIdMap(MultiTenantContext.getTenant().getId(), id);
    }

    @PutMapping("/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Update mapped configuration for given config id")
    public LookupIdMap updateLookupIdMap(HttpServletRequest request, @PathVariable String id,
            @RequestBody LookupIdMap lookupIdMap) {
        try {
            return lookupIdMappingProxy.updateLookupIdMap(MultiTenantContext.getTenant().getId(), id, lookupIdMap);
        } catch (LedpException e) {
            String title = "Cannot edit connection";
            String message;

            switch (e.getCode()) {
            case LEDP_40080:
                log.error("Failed to edit connection because empty org name", e);
                message = "System name cannot be empty";
                break;

            case LEDP_40081:
                log.error("Failed to edit connection because of duplicate org name", e);
                message = "A connection with the same system name already exists";
                break;

            default:
                message = e.getMessage();
                break;
            }
            UIAction action = UIActionUtils.generateUIAction(title, View.Banner, Status.Error, message);

            throw new UIActionException(action, e.getCode());
        }
    }

    @DeleteMapping("/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Delete mapped configuration for given config id")
    public void deleteLookupIdMap(HttpServletRequest request, @PathVariable String id) {
        lookupIdMappingProxy.deleteLookupIdMap(MultiTenantContext.getTenant().getId(), id);
    }

    @PutMapping("/delete-connection/{lookupIdMapId}")
    @ResponseBody
    @ApiOperation(value = "Delete Tray solution instance, authentication, and lookupidmap")
    public void deleteConnection(HttpServletRequest request, //
            @PathVariable String lookupIdMapId, @RequestBody TraySettings traySettings) {
        lookupIdMappingProxy.deleteConnection(MultiTenantContext.getTenant().getId(), traySettings, lookupIdMapId);
    }

    @GetMapping("/available-lookup-ids")
    @ResponseBody
    @ApiOperation(value = "Get available lookup ids per external system type")
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(HttpServletRequest request, //
            @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = false) //
            CDLExternalSystemType externalSystemType) {
        return lookupIdMappingProxy.getAllLookupIds(MultiTenantContext.getTenant().getId(), externalSystemType);
    }

    @GetMapping("/all-external-system-types")
    @ResponseBody
    @ApiOperation(value = "Get all external system type")
    public List<CDLExternalSystemType> getAllCDLExternalSystemType(HttpServletRequest request) {
        return lookupIdMappingProxy.getAllCDLExternalSystemType(MultiTenantContext.getTenant().getId());
    }
}
