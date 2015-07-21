package com.latticeengines.propdata.api.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.EntitlementSourcePackageMap;

public interface EntitlementSourcePackageMapDao extends BaseDao<EntitlementSourcePackageMap> {

    List<EntitlementSourcePackageMap> findByPackageID(Long packageID);

    EntitlementSourcePackageMap findByContent(Long packageID, String lookupID);

}
