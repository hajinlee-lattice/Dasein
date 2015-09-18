package com.latticeengines.propdata.api.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.propdata.api.dao.DomainFeatureTableDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourceColumnsContractPackageMapDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourceColumnsPackageMapDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementSourceColumnsPackagesDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageContentMapDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageContractMapDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageDao;
import com.latticeengines.propdata.api.entitymanager.EntitlementSourceColumnEntityMgr;

@Component
public class EntitlementSourceColumnEntityMgrImpl
    extends AbstractEntitlementEntityMgr<EntitlementSourceColumnsPackages,
        EntitlementSourceColumnsPackageMap, EntitlementSourceColumnsContractPackageMap>
        implements EntitlementSourceColumnEntityMgr {

    @Autowired
    private EntitlementSourceColumnsPackagesDao entitlementSourceColumnsPackagesDao;

    @Autowired
    private EntitlementSourceColumnsContractPackageMapDao entitlementSourceColumnsContractPackageMapDao;

    @Autowired
    private EntitlementSourceColumnsPackageMapDao entitlementSourceColumnsPackageMapDao;

    @Autowired
    private DomainFeatureTableDao domainFeatureTableDao;

    @Override
    protected PackageDao<EntitlementSourceColumnsPackages> getPackageDao() {
        return entitlementSourceColumnsPackagesDao;
    }

    @Override
    protected PackageContractMapDao<EntitlementSourceColumnsContractPackageMap> getPackageContractMapDao() {
        return entitlementSourceColumnsContractPackageMapDao;
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    protected PackageContentMapDao<EntitlementSourceColumnsPackageMap> getPackageContentMapDao() {
        return entitlementSourceColumnsPackageMapDao;
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<EntitlementSourceColumnsPackageMap> getPackageSourceColumns(Long sourcePackageId) {
        return entitlementSourceColumnsPackageMapDao.getByPackageId(sourcePackageId);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public DomainFeatureTable getDomainFeatureTable(String lookupId) {
        return domainFeatureTableDao.findByLookupID(lookupId);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public EntitlementSourceColumnsPackageMap getSourceColumnPackageMag(Long packageId, String lookupId,
                                                                        String columnName) {
        return entitlementSourceColumnsPackageMapDao.findByContent(packageId, lookupId, columnName);
    }

}
