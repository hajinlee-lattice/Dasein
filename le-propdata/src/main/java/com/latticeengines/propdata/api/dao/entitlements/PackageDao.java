package com.latticeengines.propdata.api.dao.entitlements;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;

public interface PackageDao<T> extends BaseDao<T> {

    Class<T> getPackageClass();

    T getById(Long packageID);

    T getByName(String packageName);

    List<T> getByIds(List<Long> ids);

}
