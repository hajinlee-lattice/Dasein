package com.latticeengines.propdata.api.entitymanager;

import java.util.List;

public interface EntitlementEntityMgr<Package, PackageContentMap, PackageContractMap> {

    Package getEntitlementPackage(Long packageId);

    Package getEntitlementPackageByName(String packageName);

    List<Package> getAllEntitlementPackages();

    void removeEntitlementPackage(Long packageId);

    Package createEntitlementPackage(Package entitlementPackage);

    List<Package> getEntitlementPackagesForCustomer(String contractId);

    PackageContractMap addPackageContractMap(PackageContractMap packageContractMap);

    PackageContractMap findPackageContractMap(Long packageId, String contractId);

    void removePackageContractMap(PackageContractMap mapEntity);

    PackageContentMap addPackageContentMap(PackageContentMap packageContentMap);

    void removePackageContentMap(PackageContentMap packageContentMap);
}
