package com.latticeengines.propdata.api.service.impl;

import java.util.List;

import javax.annotation.PostConstruct;

import com.latticeengines.propdata.api.entitymanager.EntitlementEntityMgr;
import com.latticeengines.propdata.api.service.EntitlementService;

abstract public class AbstractEntitlementService<Package, PackageContentMap, PackageContractMap>
        implements EntitlementService<Package, PackageContentMap, PackageContractMap> {

    private EntitlementEntityMgr<Package, PackageContentMap, PackageContractMap> entityMgr;

    abstract EntitlementEntityMgr<Package, PackageContentMap, PackageContractMap> getEntityMgr();

    @PostConstruct
    protected void bindEntityMgrs() {
        entityMgr = getEntityMgr();
    }

    @Override
    public Package getEntitlementPackage(Long packageId){
        return entityMgr.getEntitlementPackage(packageId);
    }

    @Override
    public Package findEntitilementPackageByName(String packageName) {
        return entityMgr.getEntitlementPackageByName(packageName);
    }

    @Override
    public List<Package> getAllEntitlementPackages() {
        return entityMgr.getAllEntitlementPackages();
    }

    @Override
    public Package createDerivedPackage(String packageName, String packageDescription, Boolean isDefault) {
        Package pkg = constructNewPackage(packageName, packageDescription, isDefault);
        return entityMgr.createEntitlementPackage(pkg);
    }

    @Override
    public void removeEntitlementPackage(Long packageId) {
        entityMgr.removeEntitlementPackage(packageId);
    }

    @Override
    public List<Package> getEntitlementPackagesForCustomer(String contractId) {
        return entityMgr.getEntitlementPackagesForCustomer(contractId);
    }

    @Override
    public PackageContractMap grantEntitlementPackageToCustomer(Long packageId, String contractId) {
        return entityMgr.addPackageContractMap(constructPackageContractMap(packageId, contractId));
    }

    @Override
    public void revokeEntitlementPackageToCustomer(Long packageId, String contractId) {
        PackageContractMap map = entityMgr.findPackageContractMap(packageId, contractId);
        entityMgr.removePackageContractMap(map);
    }

    abstract protected Package constructNewPackage(String packageName, String packageDescription, Boolean isDefault);

    abstract protected PackageContractMap constructPackageContractMap(Long packageId, String contractId);

}
