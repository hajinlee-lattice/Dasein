package com.latticeengines.propdata.api.dao.entitlements;

import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;

public interface EntitlementSourcePackageMapDao extends PackageContentMapDao<EntitlementSourcePackageMap> {

    EntitlementSourcePackageMap findByContent(Long packageID, String lookupID);

}
