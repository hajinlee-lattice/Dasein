package com.latticeengines.propdata.api.dao.entitlements;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;

public interface PackageContractMapDao<T> extends BaseDao<T> {

    List<T> findByPackageId(Long packageId);

    List<T> findByContractId(String contractId);

    T findByIds(Long packageId, String contractId);

}
