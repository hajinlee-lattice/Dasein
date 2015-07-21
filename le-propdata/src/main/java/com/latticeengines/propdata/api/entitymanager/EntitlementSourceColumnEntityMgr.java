package com.latticeengines.propdata.api.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;

public interface EntitlementSourceColumnEntityMgr {

    EntitlementSourceColumnsPackages getSourceColumnPackage(Long pid);
    
    List<EntitlementSourceColumnsPackages> getAllSourceColumnPackages();
    
    List<EntitlementSourceColumnsPackages> getEntitledSourcePackages(String Contract_ID);
    
    List<EntitlementSourceColumnsPackageMap> getPackageSourceColumns(Long packageID);
    
    EntitlementSourceColumnsPackageMap getSourceColumnFromPackage(Long packageID
            ,String lookupID, String columnName);

    Long createSourceColumnsPackage(EntitlementSourceColumnsPackages entitlementPackage);
    
    Long assignSourceColumnToPackage(Long packageID, String lookupID, String columnName);
    
    void removeSourceColumnFromPackage(EntitlementSourceColumnsPackageMap sourcePackageMap);
    
    Long assignCustomerToSourceColumnsPackage(Long packageID,String externalID);
    
    void removeCustomerFromSourceColumnsPackage(Long packageID,String externalID);

}
