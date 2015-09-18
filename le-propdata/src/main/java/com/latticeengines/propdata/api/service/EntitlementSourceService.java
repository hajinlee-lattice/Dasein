package com.latticeengines.propdata.api.service;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;

public interface EntitlementSourceService extends EntitlementService<EntitlementSourcePackages,
        EntitlementSourcePackageMap, EntitlementSourceContractPackageMap> {

    List<EntitlementSourcePackageMap> getSourceEntitlementContents(Long packageId);

    EntitlementSourcePackageMap addSourceToPackage(Long packageId, String lookupId);

    void removeSourceFromPackage(Long packageID, String lookupId);

}
