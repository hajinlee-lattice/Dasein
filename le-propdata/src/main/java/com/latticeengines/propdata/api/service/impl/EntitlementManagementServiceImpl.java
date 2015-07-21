package com.latticeengines.propdata.api.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.entitymanager.EntitlementDerivedEntityMgr;
import com.latticeengines.propdata.api.entitymanager.EntitlementSourceColumnEntityMgr;
import com.latticeengines.propdata.api.entitymanager.EntitlementSourceEntityMgr;
import com.latticeengines.propdata.api.service.EntitlementManagementService;

@Component("EntitlementManagementService")
public class EntitlementManagementServiceImpl implements EntitlementManagementService {

    @Autowired
    private EntitlementDerivedEntityMgr entitlementDerivedEntityMgr;
    
    @Autowired
    private EntitlementSourceEntityMgr entitlementSourceEntityMgr;
    
    @Autowired
    private EntitlementSourceColumnEntityMgr entitlementSourceColumnsEntityMgr;
    
    @Override
    public List<EntitlementPackages> getDerivedEntitlementPackages(
            String contractID) {
        List<EntitlementPackages> results;
        if(contractID == null){
            results = entitlementDerivedEntityMgr.getAllDerivedPackages();
        }
        else{
            results = entitlementDerivedEntityMgr.getEntitledDerivedPackages(contractID);
        }
        return results;
    }

    @Override
    public List<DataColumnMap> getDerivedEntitlementColumns(
            Long packageID) {
        return entitlementDerivedEntityMgr.getDerivedPackageColumns(packageID);
    }

    @Override
    public Long createDerivedEntitlementPackage(String packageName,
            String packageDescription, Boolean isDefault) {
        Date now = new Date();
        if(isDefault==null) isDefault=false;
        EntitlementPackages entitlementPackage = new EntitlementPackages();
        entitlementPackage.setPackageName(packageName);
        entitlementPackage.setPackageDescription(packageDescription);
        entitlementPackage.setIs_Default(isDefault);
        entitlementPackage.setLast_Modification_Date(now);
        return entitlementDerivedEntityMgr.createDerivedPackage(entitlementPackage);
    }

    @Override
    public Long assignColumnToDerivedEntitlementPackage(Long packageID,
            String extensionName, String sourceTableName) {
        DataColumnMap dc = new DataColumnMap();
        dc = entitlementDerivedEntityMgr.getDataColumn(extensionName, sourceTableName);
        return entitlementDerivedEntityMgr.assignDerivedColumnToPackage(packageID, dc); 
    }

    @Override
    public void removeColumnFromDerivedEntitlementPackage(Long packageID,
            String extensionName, String sourceTableName) {
        DataColumnMap dc = new DataColumnMap();
        dc = entitlementDerivedEntityMgr.getDataColumn(extensionName, sourceTableName);
        entitlementDerivedEntityMgr.removeDerivedColumnFromPackage(packageID, dc); 
        
    }

    @Override
    public Long assignCustomerToDerivedEntitlementPackage(Long packageID,
            String contractID) {
        
        return entitlementDerivedEntityMgr.assignCustomerToDerivedPackage(packageID, contractID);
    }

    @Override
    public void revokeCustomerFromDerivedEntitlementPackage(
            Long packageID, String contractID) {
        entitlementDerivedEntityMgr.removeCustomerFromDerivedPackage(packageID, contractID);
        
    }

    @Override
    public List<EntitlementSourcePackages> getSourceEntitlementPackages(
            String contractID) {
        List<EntitlementSourcePackages> results;
        if(contractID == null){
            results = entitlementSourceEntityMgr.getAllSourcePackages();
        }
        else{
            results = entitlementSourceEntityMgr.getEntitledSourcePackages(contractID);
        }
        return results;
    }

    @Override
    public List<EntitlementSourcePackageMap> getSourceEntitlementContents(
            Long sourcePackageID) {
        return entitlementSourceEntityMgr.getPackageSources(sourcePackageID);
    }

    @Override
    public Long createSourceEntitlementPackage(String sourcePackageName,
            String sourcePackageDescription, Boolean isDefault) {
        Date now = new Date();
        if(isDefault==null) isDefault = false;
        EntitlementSourcePackages esp = new EntitlementSourcePackages();
        esp.setSourcePackageName(sourcePackageName);
        esp.setSourcePackageDescription(sourcePackageDescription);
        esp.setIs_Default(isDefault);
        esp.setLast_Modification_Date(now);
        return entitlementSourceEntityMgr.createSourcePackage(esp);
    }

    @Override
    public Long assignSourceToEntitlementPackage(Long sourcePackageID,
            String lookupID) {
        return entitlementSourceEntityMgr.assignSourceToPackage(sourcePackageID, lookupID);
    }

    @Override
    public void removeSourceFromEntitlementPackage(Long sourcePackageID,
            String lookupID) {
        EntitlementSourcePackageMap espm = entitlementSourceEntityMgr
                .getSourceFromPackage(sourcePackageID, lookupID);
        entitlementSourceEntityMgr.removeSourceFromPackage(espm);        
    }

    @Override
    public Long assignCustomerToSourceEntitlementPackage(
            Long sourcePackageID, String contractID) {
        return entitlementSourceEntityMgr.assignCustomerToSourcePackage(
                sourcePackageID, contractID);
    }

    @Override
    public void revokeCustomerFromSourceEntitlementPackage(
            Long sourcePackageID, String contractID) {
        entitlementSourceEntityMgr
                .removeCustomerFromSourcePackage(sourcePackageID, contractID);
    }

    @Override
    public List<EntitlementSourceColumnsPackages> getSourceColumnEntitlementPackages(
            String contractID) {
        List<EntitlementSourceColumnsPackages> results;
        if(contractID == null){
            results = entitlementSourceColumnsEntityMgr.getAllSourceColumnPackages();
        }
        else{
            results = entitlementSourceColumnsEntityMgr.getEntitledSourcePackages(contractID);
        }
        return results;
    }

    @Override
    public List<EntitlementSourceColumnsPackageMap> getSourceColumnEntitlementContents(
            Long sourceColumnPackageID) {
        return entitlementSourceColumnsEntityMgr
                .getPackageSourceColumns(sourceColumnPackageID);
    }

    @Override
    public Long createSourceColumnEntitlementPackage(
            String sourceColumnPackageName,
            String sourceColumnPackageDescription, Boolean isDefault) {
        Date now = new Date();
        if(isDefault==null) isDefault = false;
        EntitlementSourceColumnsPackages esp = new EntitlementSourceColumnsPackages();
        esp.setSourceColumnsPackageName(sourceColumnPackageName);
        esp.setSourceColumnsPackageDescription(sourceColumnPackageDescription);
        esp.setIs_Default(isDefault);
        esp.setLast_Modification_Date(now);
        return entitlementSourceColumnsEntityMgr.createSourceColumnsPackage(esp);
    }

    @Override
    public Long assignSourceColumnToEntitlementPackage(
            Long sourceColumnPackageID, String lookupID, String columnName) {
        return entitlementSourceColumnsEntityMgr
                .assignSourceColumnToPackage(sourceColumnPackageID, lookupID, columnName);
    }

    @Override
    public void removeSourceColumnFromEntitlementPackage(
            Long sourceColumnPackageID, String lookupID, String columnName) {
        EntitlementSourceColumnsPackageMap espm = entitlementSourceColumnsEntityMgr
                .getSourceColumnFromPackage(sourceColumnPackageID, lookupID,columnName);
        entitlementSourceColumnsEntityMgr.removeSourceColumnFromPackage(espm);        
        
    }

    @Override
    public Long assignCustomerToSourceColumnEntitlementPackage(
            Long sourceColumnPackageID, String contractID) {
        return entitlementSourceColumnsEntityMgr.assignCustomerToSourceColumnsPackage(
                sourceColumnPackageID, contractID);
    }

    @Override
    public void revokeCustomerFromSourceColumnEntitlementPackage(
            Long sourceColumnPackageID, String contractID) {
        entitlementSourceColumnsEntityMgr
            .removeCustomerFromSourceColumnsPackage(sourceColumnPackageID, contractID);
    }

}
