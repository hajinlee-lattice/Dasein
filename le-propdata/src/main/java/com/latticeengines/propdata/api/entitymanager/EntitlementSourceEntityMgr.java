package com.latticeengines.propdata.api.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.propdata.DomainFeatureTable;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackages;

public interface EntitlementSourceEntityMgr extends EntitlementEntityMgr<EntitlementSourcePackages,
        EntitlementSourcePackageMap, EntitlementSourceContractPackageMap> {

    List<EntitlementSourcePackageMap> getSourceEntitlementContents(Long packageId);

    DomainFeatureTable getDomainFeatureTable(String lookupId);

    EntitlementSourcePackageMap getSourcePackageMag(Long packageId, String lookupId);

}
