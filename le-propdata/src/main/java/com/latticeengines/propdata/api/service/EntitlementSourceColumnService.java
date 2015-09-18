package com.latticeengines.propdata.api.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;

public interface EntitlementSourceColumnService extends EntitlementService<EntitlementSourceColumnsPackages,
        EntitlementSourceColumnsPackageMap, EntitlementSourceColumnsContractPackageMap> {

    List<EntitlementSourceColumnsPackageMap> getPackageSourceColumns(Long packageID);

    EntitlementSourceColumnsPackageMap addColumnToPackage(Long packageId, String lookupId, String columnName);

    void removeColumnFromPackage(Long packageID, String lookupId, String columnName);

}
