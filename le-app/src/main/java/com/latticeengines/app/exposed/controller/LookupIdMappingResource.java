package com.latticeengines.app.exposed.controller;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;
import javax.servlet.http.HttpServletRequest;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.CDLConstants;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.proxy.exposed.cdl.LookupIdMappingProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;

@Api(value = "lookup-id-mapping", description = "Rest resource for lookup Id mapping")
@RestController
@RequestMapping(value = "/lookup-id-mapping")
public class LookupIdMappingResource {

    @Inject
    private LookupIdMappingProxy lookupIdMappingProxy;

    @RequestMapping(value = "", method = RequestMethod.GET)
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

    @RequestMapping(value = "/register", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register an org")
    public LookupIdMap registerExternalSystem(HttpServletRequest request, @RequestBody LookupIdMap lookupIdMap) {
        return lookupIdMappingProxy.registerExternalSystem(MultiTenantContext.getTenant().getId(), lookupIdMap);
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get mapped configuration for given config id")
    public LookupIdMap getLookupIdMap(HttpServletRequest request, @PathVariable String id) {
        return lookupIdMappingProxy.getLookupIdMap(MultiTenantContext.getTenant().getId(), id);
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update mapped configuration for given config id")
    public LookupIdMap updateLookupIdMap(HttpServletRequest request, @PathVariable String id,
            @RequestBody LookupIdMap lookupIdMap) {
        return lookupIdMappingProxy.updateLookupIdMap(MultiTenantContext.getTenant().getId(), id, lookupIdMap);
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete mapped configuration for given config id")
    public void deleteLookupIdMap(HttpServletRequest request, @PathVariable String id) {
        lookupIdMappingProxy.deleteLookupIdMap(MultiTenantContext.getTenant().getId(), id);
    }

    @RequestMapping(value = "/available-lookup-ids", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get available lookup ids per external system type")
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(HttpServletRequest request, //
            @RequestParam(value = CDLConstants.EXTERNAL_SYSTEM_TYPE, required = false) //
            CDLExternalSystemType externalSystemType) {
        return lookupIdMappingProxy.getAllLookupIds(MultiTenantContext.getTenant().getId(), externalSystemType);
    }

    @RequestMapping(value = "/all-external-system-types", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all external system type")
    public List<CDLExternalSystemType> getAllCDLExternalSystemType(HttpServletRequest request) {
        return lookupIdMappingProxy.getAllCDLExternalSystemType(MultiTenantContext.getTenant().getId());
    }
}
