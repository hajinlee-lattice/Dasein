package com.latticeengines.propdata.api.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.DataColumnMap;
import com.latticeengines.domain.exposed.propdata.EntitlementPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;

public interface EntitlementManagementService {

    List<EntitlementPackages> getDerivedEntitlementPackages(
            String contractID);

    List<DataColumnMap> getDerivedEntitlementColumns(Long packageID);

    Long createDerivedEntitlementPackage(String packageName, String packageDescription, Boolean isDefault);

    Long assignColumnToDerivedEntitlementPackage(Long packageID,
            String extensionName, String sourceTableName);

    void removeColumnFromDerivedEntitlementPackage(Long packageID,
            String extensionName, String sourceTableName);

    Long assignCustomerToDerivedEntitlementPackage(Long packageID,
            String contractID);

    void revokeCustomerFromDerivedEntitlementPackage(Long packageID,
            String contractID);

    List<EntitlementSourcePackages> getSourceEntitlementPackages(String contractID);

    List<EntitlementSourcePackageMap> getSourceEntitlementContents(
            Long sourcePackageID);

    Long createSourceEntitlementPackage(String sourcePackageName,
            String sourcePackageDescription, Boolean isDefault);

    Long assignSourceToEntitlementPackage(Long sourcePackageID,
            String lookupID);

    void removeSourceFromEntitlementPackage(Long sourcePackageID,
            String lookupID);

    Long assignCustomerToSourceEntitlementPackage(Long sourcePackageID,
            String contractID);

    void revokeCustomerFromSourceEntitlementPackage(Long sourcePackageID,
            String contractID);

    List<EntitlementSourceColumnsPackages> getSourceColumnEntitlementPackages(
            String contractID);

    List<EntitlementSourceColumnsPackageMap> getSourceColumnEntitlementContents(
            Long sourceColumnPackageID);

    Long createSourceColumnEntitlementPackage(
            String sourceColumnPackageName,
            String sourceColumnPackageDescription, Boolean isDefault);

    Long assignSourceColumnToEntitlementPackage(
            Long sourceColumnPackageID, String lookupID, String columnName);

    void removeSourceColumnFromEntitlementPackage(
            Long sourceColumnPackageID, String lookupID, String columnName);

    Long assignCustomerToSourceColumnEntitlementPackage(
            Long sourceColumnPackageID, String contractID);

    void revokeCustomerFromSourceColumnEntitlementPackage(
            Long sourceColumnPackageID, String contractID);

}
