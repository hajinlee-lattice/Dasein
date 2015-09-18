package com.latticeengines.propdata.api.dao.entitlements;

import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;

public interface EntitlementColumnMapDao extends PackageContentMapDao<EntitlementColumnMap> {

    EntitlementColumnMap findByContent(Long packageID, Long columnCalc);

}
