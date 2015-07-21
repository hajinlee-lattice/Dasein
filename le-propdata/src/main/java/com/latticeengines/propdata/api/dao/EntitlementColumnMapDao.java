package com.latticeengines.propdata.api.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.EntitlementColumnMap;

public interface EntitlementColumnMapDao extends BaseDao<EntitlementColumnMap> {

    List<EntitlementColumnMap> findByPackageID(Long packageID);

    EntitlementColumnMap findByContent(Long packageID, Long columnCalc);

}
