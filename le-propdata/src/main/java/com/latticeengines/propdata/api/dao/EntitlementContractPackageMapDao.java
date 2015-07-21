package com.latticeengines.propdata.api.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.EntitlementContractPackageMap;

public interface EntitlementContractPackageMapDao extends BaseDao<EntitlementContractPackageMap>  {

    List<EntitlementContractPackageMap> findByContractID(String contractID);

    EntitlementContractPackageMap findByContent(Long packageID,
            String externalID);

}
