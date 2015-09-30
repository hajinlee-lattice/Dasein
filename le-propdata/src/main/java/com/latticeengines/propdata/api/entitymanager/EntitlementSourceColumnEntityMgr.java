package com.latticeengines.propdata.api.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackages;

public interface EntitlementSourceColumnEntityMgr extends EntitlementEntityMgr<EntitlementSourceColumnsPackages,
        EntitlementSourceColumnsPackageMap, EntitlementSourceColumnsContractPackageMap> {

    List<EntitlementSourceColumnsPackageMap> getPackageSourceColumns(Long packageID);

    DomainFeatureTable getDomainFeatureTable(String lookupId);

    EntitlementSourceColumnsPackageMap getSourceColumnPackageMag(Long packageId, String lookupId, String columnName);
}
