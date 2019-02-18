package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

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
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "lookup-id-mapping", description = "Rest resource for lookup Id mapping")
@RestController
@RequestMapping("/customerspaces/{customerSpace}/lookup-id-mapping")
public class LookupIdMappingResource {

    @Inject
    private LookupIdMappingService lookupIdMappingService;

    @GetMapping(value = "")
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

    @PostMapping(value = "/register")
    @ResponseBody
    @ApiOperation(value = "Deregister an org")
    public LookupIdMap registerExternalSystem(@PathVariable String customerSpace,
            @RequestBody LookupIdMap lookupIdMap) {
        return lookupIdMappingService.registerExternalSystem(lookupIdMap);
    }

    @PutMapping(value = "/deregister")
    @ResponseBody
    @ApiOperation(value = "Deregister an org")
    public void deregisterExternalSystem(@PathVariable String customerSpace, @RequestBody LookupIdMap lookupIdMap) {
        lookupIdMappingService.deregisterExternalSystem(lookupIdMap);
    }

    @GetMapping(value = "/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Get mapped configuration for given config id")
    public LookupIdMap getLookupIdMap(@PathVariable String customerSpace, @PathVariable String id) {
        return lookupIdMappingService.getLookupIdMap(id);
    }

    @PutMapping(value = "/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Update mapped configuration for given config id")
    public LookupIdMap updateLookupIdMap(@PathVariable String customerSpace, @PathVariable String id,
            @RequestBody LookupIdMap lookupIdMap) {
        return lookupIdMappingService.updateLookupIdMap(id, lookupIdMap);
    }

    @DeleteMapping(value = "/config/{id}")
    @ResponseBody
    @ApiOperation(value = "Delete mapped configuration for given config id")
    public void deleteLookupIdMap(@PathVariable String customerSpace, @PathVariable String id) {
        lookupIdMappingService.deleteLookupIdMap(id);
    }

    @GetMapping(value = "/available-lookup-ids")
    @ResponseBody
    @ApiOperation(value = "Get available lookup ids per external system type")
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(@PathVariable String customerSpace, //
            @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = false) //
            CDLExternalSystemType externalSystemType) {
        return lookupIdMappingService.getAllLookupIds(externalSystemType);
    }

    @GetMapping(value = "/all-external-system-types")
    @ResponseBody
    @ApiOperation(value = "Get all external system type")
    public List<CDLExternalSystemType> getAllCDLExternalSystemType(@PathVariable String customerSpace) {
        return lookupIdMappingService.getAllCDLExternalSystemType();
    }

    @GetMapping(value = "/org/{orgId}")
    @ResponseBody
    @ApiOperation(value = "Get lookup id map from org id")
    public LookupIdMap getLookupIdMapByOrgId(@PathVariable String customerSpace,
            @PathVariable String orgId, @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = true) //
            CDLExternalSystemType externalSystemType) {
        return lookupIdMappingService.getLookupIdMapByOrgId(orgId, externalSystemType);
    }
}
