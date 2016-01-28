package com.latticeengines.propdata.api.controller.match;

import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.SimpleBooleanResponse;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.DerivedPackageRequest;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.propdata.api.service.EntitlementSourceColumnService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "sourceColumnsEntitlment", description = "REST resource for source columns entitlement package management")
@RestController
@RequestMapping("/entitlements/sourcecolumns")
public class EntitlementSourceColumnResource {

    @Autowired
    EntitlementSourceColumnService entitlementService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all source column entitlement packages")
    public List<EntitlementSourceColumnsPackages> getDerivedAttributePackagesAll(
            @RequestParam(value="contractId", required = false) String contractId){
        try {
            if (StringUtils.isEmpty(contractId)) {
                return entitlementService.getAllEntitlementPackages();
            } else {
                return entitlementService.getEntitlementPackagesForCustomer(contractId);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25003, e);
        }
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create source solumn entitlement package")
    public EntitlementSourceColumnsPackages createDerivedAttributePackage(@RequestBody DerivedPackageRequest newRequest) {
        try {
            String packageName = newRequest.getPackageName();
            String packageDesc = newRequest.getPackageDescription();
            Boolean isDefault = newRequest.getIsDefault();
            return entitlementService.createDerivedPackage(packageName, packageDesc, isDefault);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25003, e);
        }
    }

    @RequestMapping(value = "/{packageID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get source column entitlement package")
    public EntitlementSourceColumnsPackages getDerivedAttributePackage(@PathVariable Long packageID) {
        try {
            return entitlementService.getEntitlementPackage(packageID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25003, e);
        }
    }

    @RequestMapping(value = "/{packageID}/columns", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get package source columns")
    public List<EntitlementSourceColumnsPackageMap> getPackageSources(@PathVariable Long packageID) {
        try {
            return entitlementService.getPackageSourceColumns(packageID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25003, e);
        }
    }


    @RequestMapping(value = "/{packageID}/columns/{lookupId}/{columnName}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add source column to entitlement package")
    public EntitlementSourceColumnsPackageMap assignColumnToDerivedAttributePackage(@PathVariable Long packageID,
                                                                             @PathVariable String lookupId,
                                                                             @PathVariable String columnName){
        try {
            return entitlementService.addColumnToPackage(packageID, lookupId, columnName);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25003, e);
        }
    }

    @RequestMapping(value = "/{packageID}/columns/{lookupId}/{columnName}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Remove source column from entitlement package")
    public SimpleBooleanResponse removeColumnFromDerivedAttributePackage(@PathVariable Long packageID,
                                                                         @PathVariable String lookupId,
                                                                         @PathVariable String columnName){
        try {
            entitlementService.removeColumnFromPackage(packageID, lookupId, columnName);
            return SimpleBooleanResponse.successResponse();
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25003, e);
        }
    }

    @RequestMapping(value = "/{packageID}/customers/{contractID}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign entitlement to customer")
    public EntitlementSourceColumnsContractPackageMap assignCustomerToDerivedAttributePackage(@PathVariable Long packageID,
                                                                                       @PathVariable String contractID){
        try {
            return entitlementService.grantEntitlementPackageToCustomer(packageID, contractID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25003, e);
        }
    }

    @RequestMapping(value = "/{packageID}/customers/{contractID}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Revoke entitlement from customer")
    public SimpleBooleanResponse revokeEntitlementsFromDerivedAttributePackage(@PathVariable Long packageID,
                                                                               @PathVariable String contractID){
        try {
            entitlementService.revokeEntitlementPackageToCustomer(packageID, contractID);
            return SimpleBooleanResponse.successResponse();
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25003, e);
        }
    }
}
