package com.latticeengines.datacloud.etl.dao;

import java.util.List;

import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.db.exposed.dao.BaseDao;

public interface ProgressDao<P> extends BaseDao<P> {

    P findByRootOperationUid(String uid);

    List<P> findFailedProgresses(Source source);

    List<P> findUnfinishedProgresses(Source source);

    List<P> findAllOfSource(Source source);

}
