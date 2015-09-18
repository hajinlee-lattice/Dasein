package com.latticeengines.propdata.api.entitymanager.impl;

import java.util.ArrayList;
import java.util.List;

import javax.annotation.PostConstruct;

import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.HasContractId;
import com.latticeengines.domain.exposed.propdata.HasPackageId;
import com.latticeengines.domain.exposed.propdata.HasPackageName;
import com.latticeengines.propdata.api.dao.entitlements.PackageContentMapDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageContractMapDao;
import com.latticeengines.propdata.api.dao.entitlements.PackageDao;
import com.latticeengines.propdata.api.entitymanager.EntitlementEntityMgr;

abstract class AbstractEntitlementEntityMgr<
        Package extends HasPackageName,
        PackageContentMap extends HasPackageId,
        PackageContractMap extends HasPackageId & HasContractId>
        implements EntitlementEntityMgr<Package, PackageContentMap, PackageContractMap> {

    private PackageDao<Package> packageDao;
    private PackageContractMapDao<PackageContractMap> packageContractMapDao;
    private PackageContentMapDao<PackageContentMap> packageContentMapDao;

    abstract protected PackageDao<Package> getPackageDao();
    abstract protected PackageContractMapDao<PackageContractMap> getPackageContractMapDao();
    abstract protected PackageContentMapDao<PackageContentMap> getPackageContentMapDao();

    @PostConstruct
    protected void bindDaos() {
        packageDao = getPackageDao();
        packageContractMapDao = getPackageContractMapDao();
        packageContentMapDao = getPackageContentMapDao();
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public Package getEntitlementPackage(Long packageId) {
        return packageDao.getById(packageId);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public Package getEntitlementPackageByName(String packageName) {
        return packageDao.getByName(packageName);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<Package> getAllEntitlementPackages() {
        return packageDao.findAll();
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public void removeEntitlementPackage(Long packageId) {
        List<PackageContractMap> maps = packageContractMapDao.findByPackageId(packageId);
        for (PackageContractMap map: maps) {
            removePackageContractMap(map);
        }
        List<PackageContentMap> contents = packageContentMapDao.getByPackageId(packageId);
        for (PackageContentMap content: contents) {
            removePackageContentMap(content);
        }
        Package pkg = getEntitlementPackage(packageId);
        if (pkg != null) {
            packageDao.delete(pkg);
        }
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public Package createEntitlementPackage(Package entitlementPackage) {
        if (getEntitlementPackageByName(entitlementPackage.getPackageName()) != null) {
            throw new IllegalStateException("Create " + packageDao.getPackageClass().getName() + " failed: package already exists.");
        }
        packageDao.create(entitlementPackage);
        return packageDao.findByKey(entitlementPackage);
    }


    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public List<Package> getEntitlementPackagesForCustomer(String contractId) {
        List<PackageContractMap> maps = packageContractMapDao.findByContractId(contractId);
        List<Long> mapIds = new ArrayList<>();
        for (PackageContractMap map: maps) {
            mapIds.add(map.getPackageId());
        }
        return packageDao.getByIds(mapIds);
    }

    @Override
    @Transactional(value = "propdataEntitlements")
    public PackageContractMap addPackageContractMap(PackageContractMap packageContractMap) {
        packageContractMapDao.create(packageContractMap);
        Long packageId = packageContractMap.getPackageId();
        String contractId = packageContractMap.getContractId();
        return packageContractMapDao.findByIds(packageId, contractId);
    }

    @Override
    @Transactional(value = "propdataEntitlements", readOnly = true)
    public PackageContractMap findPackageContractMap(Long packageId, String contractId) {
        return packageContractMapDao.findByIds(packageId, contractId);
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public void removePackageContractMap(PackageContractMap mapEntity) {
        packageContractMapDao.delete(mapEntity);
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public PackageContentMap addPackageContentMap(PackageContentMap packageContentMap) {
        packageContentMapDao.create(packageContentMap);
        return packageContentMapDao.findByKey(packageContentMap);
    }

    @Override
    @Transactional(value = "propdataEntitlements", propagation = Propagation.REQUIRED)
    public void removePackageContentMap(PackageContentMap packageContentMap) {
        packageContentMapDao.delete(packageContentMap);
    }

}
