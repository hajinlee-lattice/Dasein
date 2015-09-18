package com.latticeengines.propdata.api.service;

import java.util.List;

public interface EntitlementService<Package, PackageContentMap, PackageContractMap> {

    Package getEntitlementPackage(Long packageId);

    Package findEntitilementPackageByName(String packageName);

    List<Package> getAllEntitlementPackages();

    Package createDerivedPackage(String packageName, String packageDescription, Boolean isDefault);

    void removeEntitlementPackage(Long packageId);

    List<Package> getEntitlementPackagesForCustomer(String contractId);

    PackageContractMap grantEntitlementPackageToCustomer(Long packageId, String externalId);

    void revokeEntitlementPackageToCustomer(Long packageId, String externalId);

}
