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
import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.service.EntitlementSourceService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "sourceEntitlment", description = "REST resource for source entitlement package management")
@RestController
@RequestMapping("/entitlements/source")
public class EntitlementSourceResource {

    @Autowired
    EntitlementSourceService entitlementService;

    @RequestMapping(value = "", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all source entitlement packages")
    public List<EntitlementSourcePackages> getDerivedAttributePackagesAll(
            @RequestParam(value="contractId", required = false) String contractId){
        try {
            if (StringUtils.isEmpty(contractId)) {
                return entitlementService.getAllEntitlementPackages();
            } else {
                return entitlementService.getEntitlementPackagesForCustomer(contractId);
            }
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25002, e);
        }
    }

    @RequestMapping(value = "", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create source entitlement package")
    public EntitlementSourcePackages createDerivedAttributePackage(@RequestBody DerivedPackageRequest newRequest) {
        try {
            String packageName = newRequest.getPackageName();
            String packageDesc = newRequest.getPackageDescription();
            Boolean isDefault = newRequest.getIsDefault();
            return entitlementService.createDerivedPackage(packageName, packageDesc, isDefault);
        } catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25002, e);
        }
    }

    @RequestMapping(value = "/{packageID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get source entitlement package")
    public EntitlementSourcePackages getDerivedAttributePackage(@PathVariable Long packageID) {
        try {
            return entitlementService.getEntitlementPackage(packageID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25002, e);
        }
    }

    @RequestMapping(value = "/{packageID}/sources", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get package sources")
    public List<EntitlementSourcePackageMap> getPackageSources(@PathVariable Long packageID) {
        try {
            return entitlementService.getSourceEntitlementContents(packageID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25002, e);
        }
    }


    @RequestMapping(value = "/{packageID}/sources/{lookupId}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Add source to entitlement package")
    public EntitlementSourcePackageMap addSourceToPackage(@PathVariable Long packageID,
                                                                             @PathVariable String lookupId){
        try {
            return entitlementService.addSourceToPackage(packageID, lookupId);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25002, e);
        }
    }

    @RequestMapping(value = "/{packageID}/sources/{lookupId}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Remove source from entitlement package")
    public SimpleBooleanResponse removeSourceFromPackage(@PathVariable Long packageID,
                                                                         @PathVariable String lookupId){
        try {
            entitlementService.removeSourceFromPackage(packageID, lookupId);
            return SimpleBooleanResponse.successResponse();
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25002, e);
        }
    }

    @RequestMapping(value = "/{packageID}/customers/{contractID}", method = RequestMethod.POST, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign entitlement to customer")
    public EntitlementSourceContractPackageMap assignCustomerToDerivedAttributePackage(@PathVariable Long packageID,
                                                                                 @PathVariable String contractID){
        try {
            return entitlementService.grantEntitlementPackageToCustomer(packageID, contractID);
        }  catch (Exception e) {
            throw new LedpException(LedpCode.LEDP_25002, e);
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
            throw new LedpException(LedpCode.LEDP_25002, e);
        }
    }
}
