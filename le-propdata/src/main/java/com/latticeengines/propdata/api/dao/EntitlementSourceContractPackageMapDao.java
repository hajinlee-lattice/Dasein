package com.latticeengines.propdata.api.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceContractPackageMap;

public interface EntitlementSourceContractPackageMapDao extends BaseDao<EntitlementSourceContractPackageMap> {

    List<EntitlementSourceContractPackageMap> findByContractID(
            String contract_ID);

    EntitlementSourceContractPackageMap findByContent(Long packageID,
            String externalID);

}
