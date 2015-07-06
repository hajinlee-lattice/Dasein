package com.latticeengines.propdata.api.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.domain.exposed.propdata.ResponseID;
import com.latticeengines.propdata.api.service.EntitlementManagementService;
import com.wordnik.swagger.annotations.Api;
import com.wordnik.swagger.annotations.ApiOperation;

@Api(value = "entitlementmanagement", description = "REST resource for entitlement management")
@RestController
@RequestMapping("/entitlements")
public class EntitlementManagementResource {

    @Autowired
    private EntitlementManagementService entitlementManagementService;

    /* Derived attributes entitlements */
    @RequestMapping(value = "/derived/{contractID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Derived Attribute entitlement package")
    public List<EntitlementPackages> getDerivedAttributePackages(@PathVariable String contractID){
        
    	return entitlementManagementService.getDerivedEntitlementPackages(contractID);
    }
    
    /* Derived attributes entitlements */
    @RequestMapping(value = "/derived", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all Derived Attribute entitlement packages")
    public List<EntitlementPackages> getDerivedAttributePackagesAll(){
        
    	return entitlementManagementService.getDerivedEntitlementPackages(null);
    }

    @RequestMapping(value = "/derived/details/{packageID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Derived Attribute package columns")
    public List<DataColumnMap> getDerivedAttributePackageColumns(@PathVariable Long packageID) {
        
        return entitlementManagementService.getDerivedEntitlementColumns(packageID);
    }
    
    @RequestMapping(value = "/derived", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create Derived Attribute entitlement package")
    public ResponseID createDerivedAttributePackage(@RequestParam(value="packageName", required=true) String packageName,
    											 @RequestParam(value="packageDescription", required=true) String packageDescription,
    											 @RequestParam(value="isDefault", required=false) Boolean isDefault){
    	
    	Long id = entitlementManagementService.createDerivedEntitlementPackage(packageName,packageDescription,isDefault);
        
        return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/derived/column/{packageID}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign Column to Derived Attribute entitlement package")
    public ResponseID assignColumnToDerivedAttributePackage(@PathVariable Long packageID,
    													 @RequestParam(value="extensionName", required=true) String extensionName,
    													 @RequestParam(value="sourceTableName", required=true) String sourceTableName){
        
    	Long id =  entitlementManagementService.assignColumnToDerivedEntitlementPackage(packageID,extensionName,sourceTableName);
    
    	return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/derived/column/{packageID}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Remove Column from Derived Attribute entitlement package")
    public void removeColumnFromDerivedAttributePackage(@PathVariable Long packageID,
    													 @RequestParam(value="extensionName", required=true) String extensionName,
    													 @RequestParam(value="sourceTableName", required=true) String sourceTableName){
        
    	entitlementManagementService.removeColumnFromDerivedEntitlementPackage(packageID,extensionName,sourceTableName);
    }
    
    @RequestMapping(value = "/derived/customer/{packageID}/{contractID}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign Customer to Derived Attribute entitlement package")
    public ResponseID assignCustomerToDerivedAttributePackage(@PathVariable Long packageID, 
    		@PathVariable String contractID){
        
    	Long id = entitlementManagementService.assignCustomerToDerivedEntitlementPackage(packageID,contractID);
    	return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/derived/customer/{packageID}/{contractID}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Revoke Customer entitlements to Derived Attribute entitlement package")
    public void revokeEntitlementsFromDerivedAttributePackage(@PathVariable Long packageID,
    		@PathVariable String contractID){
        
    	entitlementManagementService.revokeCustomerFromDerivedEntitlementPackage(packageID,contractID);
    }
    
    /* Source entitlements */
    @RequestMapping(value = "/source/{contractID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Source entitlement package")
    public List<EntitlementSourcePackages> getSourcePackages(@PathVariable String contractID){
        
    	return entitlementManagementService.getSourceEntitlementPackages(contractID);
    }
    
    @RequestMapping(value = "/source", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all Source entitlement packages")
    public List<EntitlementSourcePackages> getSourcePackagesAll(){
        
    	return entitlementManagementService.getSourceEntitlementPackages(null);
    }

    @RequestMapping(value = "/source/details/{sourcePackageID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Source package contents")
    public List<EntitlementSourcePackageMap> getSourcePackageDetails(@PathVariable Long sourcePackageID) {
        
        return entitlementManagementService.getSourceEntitlementContents(sourcePackageID);
    }
    
    @RequestMapping(value = "/source", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create Source entitlement package")
    public ResponseID createSourcePackage(@RequestParam(value="sourcePackageName", required=true) String sourcePackageName,
    								   @RequestParam(value="sourcePackageDescription", required=true) String sourcePackageDescription,
    								   @RequestParam(value="isDefault", required=false) Boolean isDefault){
        
    	Long id = entitlementManagementService.createSourceEntitlementPackage(sourcePackageName,sourcePackageDescription,isDefault);
    
    	return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/source/source/{sourcePackageID}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign source to entitlement package")
    public ResponseID assignSourceToPackage(@PathVariable Long sourcePackageID,
    									 @RequestParam(value="lookupID", required=true) String lookupID){
        
    	Long id = entitlementManagementService.assignSourceToEntitlementPackage(sourcePackageID,lookupID);
    
    	return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/source/source/{sourcePackageID}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Remove source from entitlement package")
    public void removeSourceFromPackage(@PathVariable Long sourcePackageID,
    									   @RequestParam(value="lookupID", required=true) String lookupID){
        
    	entitlementManagementService.removeSourceFromEntitlementPackage(sourcePackageID,lookupID);
    }
    
    @RequestMapping(value = "/source/customer/{sourcePackageID}/{contractID}", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign Customer to source entitlement package")
    public ResponseID assignCustomerToSourcePackage(@PathVariable Long sourcePackageID,
    		@PathVariable String contractID){
        
    	Long id = entitlementManagementService.assignCustomerToSourceEntitlementPackage(sourcePackageID,contractID);
    	return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/source/customer/{sourcePackageID}/{contractID}", method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Revoke Customer entitlements to source entitlement package")
    public void revokeEntitlementsFromSourcePackage(@PathVariable Long sourcePackageID,
    		@PathVariable String contractID){
        
    	entitlementManagementService.revokeCustomerFromSourceEntitlementPackage(sourcePackageID,contractID);
    }
    
    /* Source column entitlements */
    @RequestMapping(value = "/columns/{contractID}", method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Source column entitlement package")
    public List<EntitlementSourceColumnsPackages> getSourceColumnPackages(@PathVariable String contractID){
        
    	return entitlementManagementService.getSourceColumnEntitlementPackages(contractID);
    }
    
    @RequestMapping(value = "/columns"
    		, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get all Source column entitlement packages")
    public List<EntitlementSourceColumnsPackages> getSourceColumnPackagesAll(){
        
    	return entitlementManagementService.getSourceColumnEntitlementPackages(null);
    }

    @RequestMapping(value = "/columns/details/{sourceColumnPackageID}"
    		, method = RequestMethod.GET, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Get Source column package contents")
    public List<EntitlementSourceColumnsPackageMap> getSourceColumnPackageDetails(
    		@PathVariable Long sourceColumnPackageID) {
        
        return entitlementManagementService.getSourceColumnEntitlementContents(sourceColumnPackageID);
    }
    
    @RequestMapping(value = "/columns", method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Create Source columns entitlement package")
    public ResponseID createSourceColumnPackage(@RequestParam(value="sourceColumnPackageName", required=true) String sourceColumnPackageName,
    								   @RequestParam(value="sourceColumnPackageDescription", required=true) String sourceColumnPackageDescription,
    								   @RequestParam(value="isDefault", required=false) Boolean isDefault){
        
    	Long id = entitlementManagementService.createSourceColumnEntitlementPackage(sourceColumnPackageName,sourceColumnPackageDescription,isDefault);
    	return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/columns/column/{sourceColumnPackageID}"
    		, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign source column to entitlement package")
    public ResponseID assignSourceColumnToPackage(@PathVariable Long sourceColumnPackageID,
    									 @RequestParam(value="lookupID", required=true) String lookupID,
    									 @RequestParam(value="columnName", required=true) String columnName){
        
    	Long id = entitlementManagementService.assignSourceColumnToEntitlementPackage(sourceColumnPackageID,lookupID,columnName);
    	return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/columns/column/{sourceColumnPackageID}"
    		, method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Remove source column from entitlement package")
    public void removeSourceColumnFromPackage(@PathVariable Long sourceColumnPackageID,
    									   @RequestParam(value="lookupID", required=true) String lookupID,
    									   @RequestParam(value="columnName", required=true) String columnName){
        
    	entitlementManagementService.removeSourceColumnFromEntitlementPackage(sourceColumnPackageID,lookupID,columnName);
    }
    
    @RequestMapping(value = "/columns/customer/{sourceColumnPackageID}/{contractID}"
    		, method = RequestMethod.PUT, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Assign Customer to source column entitlement package")
    public ResponseID assignCustomerToSourceColumnPackage(@PathVariable Long sourceColumnPackageID,
    		@PathVariable String contractID){
        
    	Long id = entitlementManagementService.assignCustomerToSourceColumnEntitlementPackage(sourceColumnPackageID,contractID);
    	return new ResponseID(true,null,id);
    }
    
    @RequestMapping(value = "/columns/customer/{sourceColumnPackageID}/{contractID}"
    		, method = RequestMethod.DELETE, headers = "Accept=application/json")
    @ResponseBody
    @ApiOperation(value = "Revoke Customer entitlements to source column entitlement package")
    public void revokeEntitlementsFromSourceColumnPackage(@PathVariable Long sourceColumnPackageID,
    		@PathVariable String contractID){
        
    	entitlementManagementService.revokeCustomerFromSourceColumnEntitlementPackage(sourceColumnPackageID,contractID);
    }
}
