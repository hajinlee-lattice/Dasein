package com.latticeengines.propdata.api.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;
import com.latticeengines.propdata.api.dao.DomainFeatureTableDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourceContractPackageMapDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourcePackageMapDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourcePackagesDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageContentMapDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageContractMapDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageDao;
import com.latticeengines.propdata.api.entitymanager.EntitlementSourceEntityMgr;

@Component
public class EntitlementSourceEntityMgrImpl
    extends AbstractEntitlementEntityMgr<EntitlementSourcePackages, EntitlementSourcePackageMap,
        EntitlementSourceContractPackageMap> implements EntitlementSourceEntityMgr {

    @Autowired
    private EntitlementSourcePackagesDao entitlementSourcePackagesDao;

    @Autowired
    private EntitlementSourceContractPackageMapDao entitlementSourceContractPackageMapDao;

    @Autowired
    private EntitlementSourcePackageMapDao entitlementSourcePackageMapDao;

    @Autowired
    private DomainFeatureTableDao domainFeatureTableDao;

    @Override
    protected PackageDao<EntitlementSourcePackages> getPackageDao() {
        return entitlementSourcePackagesDao;
    }

    @Override
    protected PackageContractMapDao<EntitlementSourceContractPackageMap> getPackageContractMapDao() {
        return entitlementSourceContractPackageMapDao;
    }

    @Override
    protected PackageContentMapDao<EntitlementSourcePackageMap> getPackageContentMapDao() {
        return entitlementSourcePackageMapDao;
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<EntitlementSourcePackageMap> getSourceEntitlementContents(Long sourcePackageID) {
        return entitlementSourcePackageMapDao.getByPackageId(sourcePackageID);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public DomainFeatureTable getDomainFeatureTable(String lookupId) {
        return domainFeatureTableDao.findByLookupID(lookupId);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public EntitlementSourcePackageMap getSourcePackageMag(Long packageId, String lookupId) {
        return entitlementSourcePackageMapDao.findByContent(packageId, lookupId);
    }

}
