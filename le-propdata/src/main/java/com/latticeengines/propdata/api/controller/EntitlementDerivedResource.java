package com.latticeengines.propdata.api.controller;

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
import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.domain.exposed.propdata.ColumnInDerivedPackageRequest;
import com.latticeengines.domain.exposed.propdata.DerivedPackageRequest;
import com.latticeengines.propdata.api.service.EntitlementDerivedService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "derivedEntitlment", description = "REST resource for derived entitlement package management")
@RestController
@RequestMapping("/entitlements/derived")
public class EntitlementDerivedResource {

    @Autowired
    EntitlementDerivedService entitlementService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all derived entitlement packages")
    public List<EntitlementPackages> getDerivedAttributePackagesAll(
            @RequestParam(value="contractId", required = false) String contractId){
        try {
            if (StringUtils.isEmpty(contractId)) {
                return entitlementService.getAllEntitlementPackages();
            } else {
                return entitlementService.getEntitlementPackagesForCustomer(contractId);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25001, e);
        }
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create derived entitlement package")
    public EntitlementPackages createDerivedAttributePackage(@RequestBody DerivedPackageRequest newRequest) {
        try {
            String packageName = newRequest.getPackageName();
            String packageDesc = newRequest.getPackageDescription();
            Boolean isDefault = newRequest.getIsDefault();
            return entitlementService.createDerivedPackage(packageName, packageDesc, isDefault);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25001, e);
        }
    }

    @RequestMapping(value = "/{packageID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get derived entitlement package")
    public EntitlementPackages getDerivedAttributePackage(@PathVariable Long packageID) {
        try {
            return entitlementService.getEntitlementPackage(packageID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25001, e);
        }
    }

    @RequestMapping(value = "/{packageID}/columns", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get package columns")
    public List<DataColumnMap> getDerivedAttributePackageColumns(@PathVariable Long packageID) {
        try {
            return entitlementService.getDerivedColumns(packageID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25001, e);
        }
    }


    @RequestMapping(value = "/{packageID}/columns", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add derived column to entitlement package")
    public EntitlementColumnMap assignColumnToDerivedAttributePackage(
            @PathVariable Long packageID, @RequestBody ColumnInDerivedPackageRequest newRequest){
        try {
            String extensionName = newRequest.getExtensionName();
            String sourceTableName = newRequest.getSourceTableName();
            return entitlementService.addDerivedColumnToPackage(packageID, extensionName, sourceTableName);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25001, e);
        }
    }

    @RequestMapping(value = "/{packageID}/columns", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Remove dervied column from entitlement package")
    public SimpleBooleanResponse removeColumnFromDerivedAttributePackage(@PathVariable Long packageID,
                                                         @RequestParam(value="extensionName", required=true) String extensionName,
                                                         @RequestParam(value="sourceTableName", required=true) String sourceTableName){
        try {
            entitlementService.removeDerivedColumnFromPackage(packageID, extensionName, sourceTableName);
            return SimpleBooleanResponse.successResponse();
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25001, e);
        }
    }

    @RequestMapping(value = "/{packageID}/customers/{contractID}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign entitlement to customer")
    public EntitlementContractPackageMap assignCustomerToDerivedAttributePackage(@PathVariable Long packageID,
                                                                                 @PathVariable String contractID){
        try {
            return entitlementService.grantEntitlementPackageToCustomer(packageID, contractID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25001, e);
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
            throw new LedpException(LedpCode.LEDP_25001, e);
        }
    }
}
