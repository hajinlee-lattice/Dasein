package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.apache.hadoop.yarn.api.records.ApplicationId;
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

import com.latticeengines.apps.cdl.service.LookupIdMappingService;
import com.latticeengines.apps.cdl.workflow.PublishAccountLookupWorkflowSubmitter;
import com.latticeengines.common.exposed.workflow.annotation.WorkflowPidWrapper;
import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.domain.exposed.remote.tray.TraySettings;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "lookup-id-mapping", description = "Rest resource for lookup Id mapping")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/lookup-id-mapping")
public class LookupIdMappingResource {

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @Inject
    private PublishAccountLookupWorkflowSubmitter publishAccountLookupWorkflowSubmitter;

    @GetMapping
    @ResponseBody
    @ApiOperation(value = "Get mapped configirations of org id and corresponding lookup id per external system type")
    public Map<String, List<LookupIdMap>> getLookupIdsMapping(@PathVariable String customerSpace, //
            @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = false) //
            CDLExternalSystemType externalSystemType, //
            @ApiParam(value = "Sort by", required = false) //
            @RequestParam(value = "sortby", required = false) String sortby, //
            @ApiParam(value = "Sort in descending order", required = false, defaultValue = "true") //
            @RequestParam(value = "descending", required = false, defaultValue = "true") boolean descending) {
        return lookupIdMappingService.getLookupIdsMapping(externalSystemType, sortby, descending);
    }

    @PostMapping("/register")
    @ResponseBody
    @ApiOperation(value = "Register an org")
    public LookupIdMap registerExternalSystem(@PathVariable String customerSpace,
            @RequestBody LookupIdMap lookupIdMap) {
        return lookupIdMappingService.registerExternalSystem(lookupIdMap);
    }

    @PutMapping("/deregister")
    @ResponseBody
    @ApiOperation(value = "Deregister an org")
    public void deregisterExternalSystem(@PathVariable String customerSpace, @RequestBody LookupIdMap lookupIdMap) {
        lookupIdMappingService.deregisterExternalSystem(lookupIdMap);
    }

    @GetMapping("/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Get mapped configuration for given config id")
    public LookupIdMap getLookupIdMap(@PathVariable String customerSpace, @PathVariable String id) {
        return lookupIdMappingService.getLookupIdMap(id);
    }

    @PutMapping("/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Update mapped configuration for given config id")
    public LookupIdMap updateLookupIdMap(@PathVariable String customerSpace, @PathVariable String id,
            @RequestBody LookupIdMap lookupIdMap) {
        return lookupIdMappingService.updateLookupIdMap(id, lookupIdMap);
    }

    @DeleteMapping("/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Delete mapped configuration for given config id")
    public void deleteLookupIdMap(@PathVariable String customerSpace, @PathVariable String id) {
        lookupIdMappingService.deleteLookupIdMap(id);
    }

    @PutMapping("/delete-connection/{lookupIdMapId}")
    @ResponseBody
    @ApiOperation(value = "Delete Tray solution instance, authentication, and lookupidmap")
    public void deleteConnection(@PathVariable String customerSpace, @PathVariable String lookupIdMapId,
            @RequestBody TraySettings traySettings) {
        lookupIdMappingService.deleteConnection(lookupIdMapId, traySettings);
    }

    @GetMapping("/available-lookup-ids/{audienceType}")
    @ResponseBody
    @ApiOperation(value = "Get available lookup ids per external system type")
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(@PathVariable String customerSpace, //
            @PathVariable String audienceType, //
            @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = false) //
            CDLExternalSystemType externalSystemType) {
        return lookupIdMappingService.getAllLookupIdsByAudienceType(externalSystemType, audienceType);
    }

    @GetMapping("/all-external-system-types")
    @ResponseBody
    @ApiOperation(value = "Get all external system type")
    public List<CDLExternalSystemType> getAllCDLExternalSystemType(@PathVariable String customerSpace) {
        return lookupIdMappingService.getAllCDLExternalSystemType();
    }

    @GetMapping("/org/{orgId}")
    @ResponseBody
    @ApiOperation(value = "Get lookup id map from org id")
    public LookupIdMap getLookupIdMapByOrgId(@PathVariable String customerSpace, @PathVariable String orgId,
            @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = true) //
            CDLExternalSystemType externalSystemType) {
        return lookupIdMappingService.getLookupIdMapByOrgId(orgId, externalSystemType);
    }

    @PostMapping("/publishAccountLookup")
    @ApiOperation(value = "publish/re-publish account lookup table to dynamo")
    public ResponseDocument<String> publishAccountLookup(@PathVariable(value = "customerSpace") String tenantId, @RequestBody(required = false) String targetSignature) {
        CustomerSpace customerSpace = CustomerSpace.parse(tenantId);
        try {
            ApplicationId applicationId = publishAccountLookupWorkflowSubmitter.submit(customerSpace, targetSignature, new WorkflowPidWrapper(-1L));
            return ResponseDocument.successResponse(applicationId.toString());
        } catch (RuntimeException e) {
            return ResponseDocument.failedResponse(e);
        }
    }
}
