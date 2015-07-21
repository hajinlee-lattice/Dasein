package com.latticeengines.propdata.api.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsPackageMap;

public interface EntitlementSourceColumnsPackageMapDao extends BaseDao<EntitlementSourceColumnsPackageMap> {

    List<EntitlementSourceColumnsPackageMap> findByPackageID(Long packageID);

    EntitlementSourceColumnsPackageMap findByContent(Long packageID,
            String lookupID, String columnName);

}
