package com.latticeengines.propdata.collection.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;

public interface ArchiveProgressDao<T> extends BaseDao<T> {

    T findByRootOperationUid(String uid);

    List<T> findFailedProgresses();

}
