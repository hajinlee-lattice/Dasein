package com.latticeengines.propdata.collection.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.propdata.collection.source.Source;

public interface ProgressDao<P> extends BaseDao<P> {

    P findByRootOperationUid(String uid);

    List<P> findFailedProgresses(Source source);

    List<P> findAllOfSource(Source source);

}
