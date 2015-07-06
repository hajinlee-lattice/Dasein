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
import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.dao.DomainFeatureTableDao;
import com.latticeengines.propdata.api.dao.EntitlementSourceContractPackageMapDao;
import com.latticeengines.propdata.api.dao.EntitlementSourcePackageMapDao;
import com.latticeengines.propdata.api.dao.EntitlementSourcePackagesDao;
import com.latticeengines.propdata.api.entitymanager.EntitlementSourceEntityMgr;

@Component("entitlementSourceEntityMgrImpl")
public class EntitlementSourceEntityMgrImpl implements EntitlementSourceEntityMgr {

	private final Log log = LogFactory.getLog(this.getClass());
	
	@Autowired
	private EntitlementSourcePackagesDao entitlementSourcePackagesDao;
	
	@Autowired
	private EntitlementSourcePackageMapDao entitlementSourcePackageMapDao;
	
	@Autowired
	private EntitlementSourceContractPackageMapDao entitlementSourceContractPackageMapDao;
	
	@Autowired
	private DomainFeatureTableDao domainFeatureTableDao;
	
	public EntitlementSourceEntityMgrImpl() {
		super();
	}

	@Override
	@Transactional(value = "propdataEntitlements", readOnly = true)
	public EntitlementSourcePackages getSourcePackage(Long pid) {
		return entitlementSourcePackagesDao.findByKey(EntitlementSourcePackages.class, pid);
	}

	@Override
	@Transactional(value = "propdataEntitlements", readOnly = true)
	public List<EntitlementSourcePackages> getAllSourcePackages() {
		return entitlementSourcePackagesDao.findAll();
	}

	@Override
	@Transactional(value = "propdataEntitlements", readOnly = true)
	public List<EntitlementSourcePackages> getEntitledSourcePackages(
			String Contract_ID) {
		List<EntitlementSourcePackages> results = new ArrayList<EntitlementSourcePackages>();
		List<EntitlementSourceContractPackageMap> contractMaps = 
				entitlementSourceContractPackageMapDao.findByContractID(Contract_ID);
		for(EntitlementSourceContractPackageMap contractMap:contractMaps){
			results.add(entitlementSourcePackagesDao
					.findByKey(EntitlementSourcePackages.class, contractMap.getSourcePackage_ID()));
		}
		return (List<EntitlementSourcePackages>)results;
	}

	@Override
	@Transactional(value = "propdataEntitlements", readOnly = true)
	public List<EntitlementSourcePackageMap> getPackageSources(Long packageID) {
		return entitlementSourcePackageMapDao.findByPackageID(packageID);
	}

	@Override
	@Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
	public Long createSourcePackage(EntitlementSourcePackages entitlementPackage) {
		entitlementSourcePackagesDao.create(entitlementPackage);
		return entitlementPackage.getPid();
	}

	@Override
	@Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
	public Long assignSourceToPackage(
			Long packageID, String lookupID) {
		Date now = new Date();
		EntitlementSourcePackages entitlementPackage = 
				entitlementSourcePackagesDao.findByKey(EntitlementSourcePackages.class, packageID);
		if (entitlementPackage == null) {
            log.error("There's no package for :" + entitlementPackage);
            throw new IllegalStateException("There's no package specified.");
        }
		
		Boolean isDomainBased;
		DomainFeatureTable dft = 
				domainFeatureTableDao.findByLookupID(lookupID);
		if(dft == null) isDomainBased = false;
		else isDomainBased = true;
		
		EntitlementSourcePackageMap escpm = new EntitlementSourcePackageMap();
		escpm.setEntitlementSourcePackage(entitlementPackage);
		escpm.setSourcePackage_ID(entitlementPackage.getPid());
		escpm.setLookup_ID(lookupID);
		escpm.setLast_Modification_Date(now);
		escpm.setIsDomainBased(isDomainBased);
		entitlementSourcePackageMapDao.create(escpm);
		return escpm.getPid();
	}

	@Override
	@Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
	public void removeSourceFromPackage(
			EntitlementSourcePackageMap sourcePackageMap) {
		entitlementSourcePackageMapDao.delete(sourcePackageMap);
	}

	@Override
	@Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
	public Long assignCustomerToSourcePackage(Long packageID, String externalID) {
		Date now = new Date();
		EntitlementSourcePackages entitlementPackage = 
				entitlementSourcePackagesDao.findByKey(EntitlementSourcePackages.class, packageID);
		if (entitlementPackage == null) {
            log.error("There's no package for :" + entitlementPackage);
            throw new IllegalStateException("There's no package specified.");
        }
		EntitlementSourceContractPackageMap escpm = new EntitlementSourceContractPackageMap();
		escpm.setContract_ID(externalID);
		escpm.setEntitlementSourcePackage(entitlementPackage);
		escpm.setSourcePackage_ID(entitlementPackage.getPid());
		escpm.setLast_Modification_Date(now);
		entitlementSourceContractPackageMapDao.create(escpm);
		return escpm.getPid();
	}

	@Override
	@Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
	public void removeCustomerFromSourcePackage(Long packageID,
			String externalID) {
		EntitlementSourceContractPackageMap ecpm = 
				entitlementSourceContractPackageMapDao.findByContent(packageID,externalID);
		entitlementSourceContractPackageMapDao.delete(ecpm);
	}

	@Override
	@Transactional(value = "propdataEntitlements", readOnly = true)
	public EntitlementSourcePackageMap getSourceFromPackage(Long packageID,
			String lookupID) {
		return entitlementSourcePackageMapDao.findByContent(packageID,lookupID);
	}

}
