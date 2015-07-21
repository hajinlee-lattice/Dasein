package com.latticeengines.propdata.api.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;

public interface EntitlementSourceEntityMgr {

    EntitlementSourcePackages getSourcePackage(Long pid);
    
    List<EntitlementSourcePackages> getAllSourcePackages();
    
    List<EntitlementSourcePackages> getEntitledSourcePackages(String Contract_ID);
    
    List<EntitlementSourcePackageMap> getPackageSources(Long packageID);
    
    EntitlementSourcePackageMap getSourceFromPackage(Long packageID,String lookupID);

    Long createSourcePackage(EntitlementSourcePackages entitlementPackage);
    
    Long assignSourceToPackage(Long packageID, String lookupID);
    
    void removeSourceFromPackage(EntitlementSourcePackageMap sourcePackageMap);
    
    Long assignCustomerToSourcePackage(Long packageID,String externalID);
    
    void removeCustomerFromSourcePackage(Long packageID,String externalID);
    
}
