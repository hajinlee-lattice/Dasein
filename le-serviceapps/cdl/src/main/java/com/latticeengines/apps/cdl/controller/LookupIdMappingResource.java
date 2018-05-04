package com.latticeengines.apps.cdl.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
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

    @RequestMapping(value = "", method = RequestMethod.GET)
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

    @RequestMapping(value = "/register", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register an org")
    public LookupIdMap registerExternalSystem(@PathVariable String customerSpace,
            @RequestBody LookupIdMap lookupIdsMap) {
        return lookupIdMappingService.registerExternalSystem(lookupIdsMap);
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get mapped configuration for given config id")
    public LookupIdMap getLookupIdMap(@PathVariable String customerSpace, @PathVariable String id) {
        return lookupIdMappingService.getLookupIdMap(id);
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update mapped configuration for given config id")
    public LookupIdMap updateLookupIdMap(@PathVariable String customerSpace, @PathVariable String id,
            @RequestBody LookupIdMap lookupIdMap) {
        return lookupIdMappingService.updateLookupIdMap(id, lookupIdMap);
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete mapped configuration for given config id")
    public void deleteLookupIdMap(@PathVariable String customerSpace, @PathVariable String id) {
        lookupIdMappingService.deleteLookupIdMap(id);
    }

    @RequestMapping(value = "/available-lookup-ids", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get available lookup ids per external system type")
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(@PathVariable String customerSpace, //
            @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = false) //
            CDLExternalSystemType externalSystemType) {
        return lookupIdMappingService.getAllLookupIds(externalSystemType);
    }

    @RequestMapping(value = "/all-external-system-types", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all external system type")
    public List<CDLExternalSystemType> getAllCDLExternalSystemType(@PathVariable String customerSpace) {
        return lookupIdMappingService.getAllCDLExternalSystemType();
    }
}
