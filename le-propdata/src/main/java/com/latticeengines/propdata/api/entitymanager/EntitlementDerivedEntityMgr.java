package com.latticeengines.propdata.api.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;

public interface EntitlementDerivedEntityMgr {
    
    EntitlementPackages getDerivedPackage(Long pid);
    
    List<EntitlementPackages> getAllDerivedPackages();
    
    List<EntitlementPackages> getEntitledDerivedPackages(String Contract_ID);
    
    List<DataColumnMap> getDerivedPackageColumns(Long packageID);

    Long createDerivedPackage(EntitlementPackages entitlementPackage);
    
    DataColumnMap getDataColumn(String extensionName,String sourceTableName);
    
    Long assignDerivedColumnToPackage(Long packageID,DataColumnMap dataColumn);
    
    void removeDerivedColumnFromPackage(Long packageID,DataColumnMap dataColumn);
    
    Long assignCustomerToDerivedPackage(Long packageID,String externalID);
    
    void removeCustomerFromDerivedPackage(Long packageID,String externalID);
}
