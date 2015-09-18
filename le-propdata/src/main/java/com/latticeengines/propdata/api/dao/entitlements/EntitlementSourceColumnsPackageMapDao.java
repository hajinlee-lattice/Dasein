package com.latticeengines.propdata.api.dao.entitlements;

import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;

public interface EntitlementSourceColumnsPackageMapDao extends PackageContentMapDao<EntitlementSourceColumnsPackageMap> {

    EntitlementSourceColumnsPackageMap findByContent(Long packageID,
                                                     String lookupID, String columnName);

}
