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

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.propdata.api.dao.DataColumnMapDao;
import com.latticeengines.propdata.api.dao.EntitlementColumnMapDao;
import com.latticeengines.propdata.api.dao.EntitlementContractPackageMapDao;
import com.latticeengines.propdata.api.dao.EntitlementPackagesDao;
import com.latticeengines.propdata.api.entitymanager.EntitlementDerivedEntityMgr;

@Component("entitlementEntityMgrImpl")
public class EntitlementDerivedEntityMgrImpl implements EntitlementDerivedEntityMgr {

    private final Log log = LogFactory.getLog(this.getClass());
    
    @Autowired
    private DataColumnMapDao dataColumnMapDao;
    
    @Autowired
    private EntitlementColumnMapDao entitlementColumnMapDao;
    
    @Autowired
    private EntitlementContractPackageMapDao entitlementContractPackageMapDao;
    
    @Autowired
    private EntitlementPackagesDao entitlementPackagesDao;
    
    public EntitlementDerivedEntityMgrImpl() {
        super();
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public EntitlementPackages getDerivedPackage(Long pid) {
        return entitlementPackagesDao.findByKey(EntitlementPackages.class, pid);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<EntitlementPackages> getAllDerivedPackages() {
        return entitlementPackagesDao.findAll();
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<EntitlementPackages> getEntitledDerivedPackages(
            String contractID) {
        List<EntitlementPackages> entitlementPackages = new ArrayList<EntitlementPackages>();
        List<EntitlementContractPackageMap> contractPackageMap = 
                entitlementContractPackageMapDao.findByContractID(contractID);
        for(EntitlementContractPackageMap contractPackage:contractPackageMap){
            entitlementPackages.add(getDerivedPackage(contractPackage.getPackage_ID()));
        }
        return entitlementPackages;
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<DataColumnMap> getDerivedPackageColumns(
            Long packageID) {
        List<DataColumnMap> results = new ArrayList<DataColumnMap>();
        List<EntitlementColumnMap> entitlementColumnMaps
            = entitlementColumnMapDao.findByPackageID(packageID);
        for(EntitlementColumnMap ecm:entitlementColumnMaps){
            results.add(dataColumnMapDao.findByKey(
                    DataColumnMap.class,ecm.getColumnCalculation_ID()));
        }
        return results;
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public Long createDerivedPackage(EntitlementPackages entitlementPackage) {
        entitlementPackagesDao.create(entitlementPackage);
        return entitlementPackage.getPid();
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public DataColumnMap getDataColumn(String extensionName,
            String sourceTableName) {
        return dataColumnMapDao.findByContent(extensionName,sourceTableName);
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public Long assignDerivedColumnToPackage(Long packageID,
            DataColumnMap dataColumn) {
        Date now = new Date();
        EntitlementPackages entitlementPackage = 
                entitlementPackagesDao.findByKey(EntitlementPackages.class, packageID);
        if (entitlementPackage == null) {
            log.error("There's no package for :" + entitlementPackage);
            throw new IllegalStateException("There's no package specified.");
        }
        EntitlementColumnMap ecm = new EntitlementColumnMap();
        ecm.setColumnCalculation_ID(dataColumn.getColumnCalcID());
        ecm.setEntitlementPackage(entitlementPackage);
        ecm.setPackage_ID(entitlementPackage.getPid());
        ecm.setLast_Modification_Date(now);
        entitlementColumnMapDao.create(ecm);
        return ecm.getPid();
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public void removeDerivedColumnFromPackage(Long packageID,
            DataColumnMap dataColumn) {
        EntitlementColumnMap ecm = 
                entitlementColumnMapDao.findByContent(packageID,dataColumn.getPid());
        entitlementColumnMapDao.delete(ecm);
        
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public Long assignCustomerToDerivedPackage(Long packageID, String externalID) {
        Date now = new Date();
        EntitlementPackages entitlementPackage = 
                entitlementPackagesDao.findByKey(EntitlementPackages.class, packageID);
        if (entitlementPackage == null) {
            log.error("There's no package for :" + entitlementPackage);
            throw new IllegalStateException("There's no package specified.");
        }
        EntitlementContractPackageMap ecpm = new EntitlementContractPackageMap();
        ecpm.setContract_ID(externalID);
        ecpm.setEntitlementPackage(entitlementPackage);
        ecpm.setPackage_ID(entitlementPackage.getPid());
        ecpm.setLast_Modification_Date(now);
        entitlementContractPackageMapDao.create(ecpm);
        return ecpm.getPid();
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public void removeCustomerFromDerivedPackage(Long packageID,
            String externalID) {
        EntitlementContractPackageMap ecpm = 
                entitlementContractPackageMapDao.findByContent(packageID,externalID);
        entitlementContractPackageMapDao.delete(ecpm);
        
    }

}
