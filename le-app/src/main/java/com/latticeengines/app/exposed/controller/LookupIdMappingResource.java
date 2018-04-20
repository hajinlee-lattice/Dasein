package com.latticeengines.app.exposed.controller;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.inject.Inject;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemMapping;
import com.latticeengines.domain.exposed.cdl.CDLExternalSystemType;
import com.latticeengines.domain.exposed.pls.LookupIdMap;
import com.latticeengines.proxy.exposed.cdl.CDLExternalSystemProxy;

import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;

@Api(value = "lookup-id-mapping", description = "Rest resource for lookup Id mapping")
@RestController
@RequestMapping(value = "/lookup-id-mapping")
public class LookupIdMappingResource {

    @Inject
    private CDLExternalSystemProxy cdlExternalSystemProxy;

    @RequestMapping(value = "", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get mapped configirations of org id and corresponding lookup id per external system type")
    public Map<String, List<LookupIdMap>> getLookupIdsMapping(HttpServletRequest request, //
            @RequestParam(value = "externalSystemType", required = false) //
            CDLExternalSystemType externalSystemType) {
        Map<String, List<LookupIdMap>> lookupIdsMapping = createDummyMapping(externalSystemType);
        return lookupIdsMapping;
    }

    @RequestMapping(value = "/register", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Register an org")
    public void registerExterSystem(HttpServletRequest request, @RequestBody LookupIdMap lookupIdsMap) {
        if (lookupIdsMap != null //
                && StringUtils.isNotBlank(lookupIdsMap.getOrgId()) //
                && StringUtils.isNotBlank(lookupIdsMap.getOrgName()) //
                && lookupIdsMap.getExternalSystemType() != null) {
            // do nothing for now

            // this api is supposed to be called from external system only to
            // get those registered. Make sure to check if this org id is
            // already registered or not. If it exists, just allow update of org
            // name
        } else {
            throw new RuntimeException(
                    "Incorrect input payload. Will replace this exception with proper LEDP exception.");
        }
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get mapped configuration for given config id")
    public LookupIdMap getLookupIdMap(HttpServletRequest request, @PathVariable String id) {
        return findExistingLookupIdMap(id);
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Update mapped configuration for given config id")
    public LookupIdMap updateLookupIdMap(HttpServletRequest request, @PathVariable String id,
            @RequestBody LookupIdMap lookupIdMap) {
        LookupIdMap existingLookupIdMap = findExistingLookupIdMap(id);
        if (existingLookupIdMap != null) {
            if (lookupIdMap != null) {
                existingLookupIdMap.setAccountId(lookupIdMap.getAccountId());
                existingLookupIdMap.setUpdated(new Date(System.currentTimeMillis()));
            } else {
                throw new RuntimeException(
                        "Incorrect input payload. Will replace this exception with proper LEDP exception.");
            }
        } else {
            throw new RuntimeException(String.format("No registration exists for id %s yet, update not allowed. "
                    + "Will replace this exception with proper LEDP exception.", id));
        }
        return existingLookupIdMap;
    }

    @RequestMapping(value = "/config/{id}", method = RequestMethod.DELETE)
    @ResponseBody
    @ApiOperation(value = "Delete mapped configuration for given config id")
    public void deleteLookupIdMap(HttpServletRequest request, @PathVariable String id) {
    }

    @RequestMapping(value = "/available-lookup-ids", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get available lookup ids per external system type")
    public Map<String, List<CDLExternalSystemMapping>> getAllLookupIds(HttpServletRequest request, //
            @RequestParam(value = "externalSystemType", required = false) //
            CDLExternalSystemType externalSystemType) {
        CustomerSpace space = MultiTenantContext.getCustomerSpace();
        Map<String, List<CDLExternalSystemMapping>> result = null;
        if (externalSystemType == null) {
            cdlExternalSystemProxy.getExternalSystemMap(space.toString());
        } else {
            result = new HashMap<>();
            result.put(externalSystemType.name(),
                    cdlExternalSystemProxy.getExternalSystemByType(space.toString(), externalSystemType));
        }

        return result;
    }

    @RequestMapping(value = "/all-external-system-types", method = RequestMethod.GET)
    @ResponseBody
    @ApiOperation(value = "Get all external system type")
    public List<CDLExternalSystemType> getAllCDLExternalSystemType(HttpServletRequest request) {
        return Arrays.asList(CDLExternalSystemType.values());
    }

    private LookupIdMap findExistingLookupIdMap(String id) {
        Map<String, List<LookupIdMap>> existingLookupIdMapping = createDummyMapping(null);
        return existingLookupIdMapping.keySet().stream() //
                .flatMap(x -> existingLookupIdMapping.get(x).stream()) //
                .filter(l -> l.getId().equals(id)) //
                .findAny() //
                .orElse(null);
    }

    private Map<String, List<LookupIdMap>> createDummyMapping(CDLExternalSystemType externalSystemType) {
        LookupIdMap idMap1 = createDummyIdMap(1L, "SFDC_SAND_ABC", "My Salesforce Sandbox", CDLExternalSystemType.CRM,
                "Acc_Id_1", null);
        LookupIdMap idMap2 = createDummyIdMap(2L, "SFDC_PROD_PQR", "My Salesforce Prod", CDLExternalSystemType.CRM,
                "Acc_Id_2", null);
        LookupIdMap idMap3 = createDummyIdMap(3L, "SFDC_SAND_XYZ", "My Another Salesforce Sandbox",
                CDLExternalSystemType.CRM, "Acc_Id_1", null);
        LookupIdMap idMap4 = createDummyIdMap(1L, "ELQ_SAND_ABC", "My Salesforce Sandbox", CDLExternalSystemType.CRM,
                "Acc_Id_3", null);
        LookupIdMap idMap5 = createDummyIdMap(2L, "ELQ_PROD_PQR", "My Salesforce Prod", CDLExternalSystemType.CRM,
                "Acc_Id_4", null);
        LookupIdMap idMap6 = createDummyIdMap(3L, "OTHER_SAND_XYZ", "My Another Salesforce Sandbox",
                CDLExternalSystemType.CRM, "Acc_Id_5", null);

        Map<String, List<LookupIdMap>> result = new HashMap<>();
        result.put(CDLExternalSystemType.CRM.name(), Arrays.asList(idMap1, idMap2, idMap3));
        result.put(CDLExternalSystemType.MAP.name(), Arrays.asList(idMap4, idMap5));
        result.put(CDLExternalSystemType.OTHER.name(), Arrays.asList(idMap6));

        if (externalSystemType != null) {
            List<LookupIdMap> list = result.get(externalSystemType.name());
            result = new HashMap<>();
            result.put(externalSystemType.name(), list);
        }

        return result;
    }

    private LookupIdMap createDummyIdMap(Long pid, String orgId, String orgName,
            CDLExternalSystemType externalSystemType, String accountId, String description) {
        LookupIdMap idMap = new LookupIdMap();
        idMap.setPid(pid);
        idMap.setId(String.format("id_%d", orgId.hashCode()));
        idMap.setOrgId(orgId);
        idMap.setOrgName(orgName);
        idMap.setExternalSystemType(externalSystemType);
        idMap.setCreated(new Date(System.currentTimeMillis()));
        idMap.setUpdated(new Date(System.currentTimeMillis()));
        idMap.setAccountId(accountId);
        idMap.setDescription(description);
        return idMap;
    }
}
