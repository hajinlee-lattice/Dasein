package com.latticeengines.propdata.api.dao.entitlements;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;

public interface PackageContentMapDao<T> extends BaseDao<T> {

    List<T> getByPackageId(Long packageID);

}
