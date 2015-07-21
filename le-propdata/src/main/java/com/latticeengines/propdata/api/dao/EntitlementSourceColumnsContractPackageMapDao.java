package com.latticeengines.propdata.api.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.EntitlementSourceColumnsContractPackageMap;

public interface EntitlementSourceColumnsContractPackageMapDao extends
        BaseDao<EntitlementSourceColumnsContractPackageMap> {

    List<EntitlementSourceColumnsContractPackageMap> findByContractID(
            String contract_ID);

    EntitlementSourceColumnsContractPackageMap findByContent(Long packageID, String externalID);

}
