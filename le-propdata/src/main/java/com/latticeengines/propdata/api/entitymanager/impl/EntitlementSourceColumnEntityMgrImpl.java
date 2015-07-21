package com.latticeengines.propdata.api.entitymanager.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.propdata.api.dao.DomainFeatureTableDao;
import com.latticeengines.propdata.api.dao.EntitlementSourceColumnsContractPackageMapDao;
import com.latticeengines.propdata.api.dao.EntitlementSourceColumnsPackageMapDao;
import com.latticeengines.propdata.api.dao.EntitlementSourceColumnsPackagesDao;
import com.latticeengines.propdata.api.entitymanager.EntitlementSourceColumnEntityMgr;

@Component("entitlementSourceColumnEntityMgrImpl")
public class EntitlementSourceColumnEntityMgrImpl implements
        EntitlementSourceColumnEntityMgr {
    
    private final Log log = LogFactory.getLog(this.getClass());
    
    @Autowired
    private EntitlementSourceColumnsPackagesDao entitlementSourceColumnsPackagesDao;
    
    @Autowired
    private EntitlementSourceColumnsPackageMapDao entitlementSourceColumnsPackageMapDao;
    
    @Autowired
    private EntitlementSourceColumnsContractPackageMapDao entitlementSourceColumnsContractPackageMapDao;

    @Autowired
    private DomainFeatureTableDao domainFeatureTableDao;
    
    public EntitlementSourceColumnEntityMgrImpl() {
        super();
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public EntitlementSourceColumnsPackages getSourceColumnPackage(Long pid) {
        return entitlementSourceColumnsPackagesDao
                .findByKey(EntitlementSourceColumnsPackages.class,pid);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<EntitlementSourceColumnsPackages> getAllSourceColumnPackages() {
        return entitlementSourceColumnsPackagesDao.findAll();
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<EntitlementSourceColumnsPackages> getEntitledSourcePackages(
            String Contract_ID) {
        List<EntitlementSourceColumnsPackages> results = new ArrayList<EntitlementSourceColumnsPackages>();
        List<EntitlementSourceColumnsContractPackageMap> contractMaps = 
                entitlementSourceColumnsContractPackageMapDao.findByContractID(Contract_ID);
        for(EntitlementSourceColumnsContractPackageMap contractMap:contractMaps){
            results.add(entitlementSourceColumnsPackagesDao
                    .findByKey(EntitlementSourceColumnsPackages.class, contractMap.getSourceColumnsPackage_ID()));
        }
        return results;
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<EntitlementSourceColumnsPackageMap> getPackageSourceColumns(
            Long packageID) {
        return entitlementSourceColumnsPackageMapDao.findByPackageID(packageID);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public EntitlementSourceColumnsPackageMap getSourceColumnFromPackage(
            Long packageID, String lookupID, String columnName) {
        return entitlementSourceColumnsPackageMapDao.findByContent(packageID,lookupID,columnName);
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public Long createSourceColumnsPackage(
            EntitlementSourceColumnsPackages entitlementPackage) {
        entitlementSourceColumnsPackagesDao.create(entitlementPackage);
        return entitlementPackage.getPid();
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public Long assignSourceColumnToPackage(
            Long packageID, String lookupID, String columnName) {
        Date now = new Date();
        EntitlementSourceColumnsPackages entitlementPackage = 
                entitlementSourceColumnsPackagesDao
                .findByKey(EntitlementSourceColumnsPackages.class, packageID);
        if (entitlementPackage == null) {
            log.error("There's no package for :" + entitlementPackage);
            throw new IllegalStateException("There's no package specified.");
        }
        
        Boolean isDomainBased;
        DomainFeatureTable dft = 
                domainFeatureTableDao.findByLookupID(lookupID);
        if(dft == null) isDomainBased = false;
        else isDomainBased = true;
        
        EntitlementSourceColumnsPackageMap escpm = new EntitlementSourceColumnsPackageMap();
        escpm.setColumnName(columnName);
        escpm.setLookup_ID(lookupID);
        escpm.setEntitlementSourceColumnsPackage(entitlementPackage);
        escpm.setSourceColumnsPackage_ID(entitlementPackage.getPid());
        escpm.setLast_Modification_Date(now);
        escpm.setIsDomainBased(isDomainBased);
        entitlementSourceColumnsPackageMapDao.create(escpm);
        return escpm.getPid();
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public void removeSourceColumnFromPackage(
            EntitlementSourceColumnsPackageMap sourcePackageMap) {
        entitlementSourceColumnsPackageMapDao.delete(sourcePackageMap);
        
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public Long assignCustomerToSourceColumnsPackage(Long packageID,
            String externalID) {
        Date now = new Date();
        EntitlementSourceColumnsPackages entitlementPackage = 
                entitlementSourceColumnsPackagesDao.findByKey(EntitlementSourceColumnsPackages.class
                        , packageID);
        if (entitlementPackage == null) {
            log.error("There's no package for :" + entitlementPackage);
            throw new IllegalStateException("There's no package specified.");
        }
        EntitlementSourceColumnsContractPackageMap escpm = new EntitlementSourceColumnsContractPackageMap();
        escpm.setContract_ID(externalID);
        escpm.setEntitlementSourceColumnsPackage(entitlementPackage);
        escpm.setSourceColumnsPackage_ID(entitlementPackage.getPid());
        escpm.setLast_Modification_Date(now);
        entitlementSourceColumnsContractPackageMapDao.create(escpm);
        return escpm.getPid();
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public void removeCustomerFromSourceColumnsPackage(Long packageID,
            String externalID) {
        EntitlementSourceColumnsContractPackageMap escpm = 
                entitlementSourceColumnsContractPackageMapDao.findByContent(packageID,externalID);
        entitlementSourceColumnsContractPackageMapDao.delete(escpm);
    }

}
