package com.latticeengines.propdata.api.entitymanager.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.propdata.api.dao.DataColumnMapDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementColumnMapDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementContractPackageMapDao;
import com.latticeengines.propdata.api.dao.entitlements.EntitlementPackagesDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageContentMapDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageContractMapDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageDao;
import com.latticeengines.propdata.api.entitymanager.EntitlementDerivedEntityMgr;

@Component("entitlementDerviedEntityMgr")
public class EntitlementDerivedEntityMgrImpl
    extends AbstractEntitlementEntityMgr<EntitlementPackages, EntitlementColumnMap, EntitlementContractPackageMap>
    implements EntitlementDerivedEntityMgr {

    @Autowired
    private EntitlementPackagesDao packageDao;

    @Autowired
    private EntitlementContractPackageMapDao contractMapDao;

    @Autowired
    private DataColumnMapDao dataColumnMapDao;

    @Autowired
    private EntitlementColumnMapDao contentMapDao;

    @Override
    protected PackageDao<EntitlementPackages> getPackageDao() {
        return packageDao;
    }

    @Override
    protected PackageContractMapDao<EntitlementContractPackageMap> getPackageContractMapDao() {
        return contractMapDao;
    }

    @Override
    protected PackageContentMapDao<EntitlementColumnMap> getPackageContentMapDao() {
        return contentMapDao;
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED, readOnly = true)
    public List<DataColumnMap> getDataColumns(Long packageId) {
        List<EntitlementColumnMap> maps = contentMapDao.getByPackageId(packageId);
        Set<Long> colIds = new HashSet<>();
        for (EntitlementColumnMap map: maps) {
            colIds.add(map.getColumnCalculation_ID());
        }
        return dataColumnMapDao.findByColumnCalcIds(new ArrayList<>(colIds));
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED, readOnly = true)
    public DataColumnMap getDataColumn(String extensionName, String sourceTableName) {
        return dataColumnMapDao.findByContent(extensionName, sourceTableName);
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED, readOnly = true)
    public EntitlementColumnMap getColumnMap(Long packageId, Long columnCalcId) {
        return contentMapDao.findByContent(packageId, columnCalcId);
    }

}
