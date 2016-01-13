package com.latticeengines.propdata.collection.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.propdata.core.source.Source;

public interface ProgressDao<P> extends BaseDao<P> {

    P findByRootOperationUid(String uid);

    List<P> findFailedProgresses(Source source);

    List<P> findUnfinishedProgresses(Source source);

    List<P> findAllOfSource(Source source);

}
