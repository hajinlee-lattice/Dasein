package com.latticeengines.propdata.collection.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.source.Source;

public interface ArchiveProgressDao extends BaseDao<ArchiveProgress> {

    ArchiveProgress findByRootOperationUid(String uid);

    List<ArchiveProgress> findFailedProgresses(Source source);

    List<ArchiveProgress> findAllOfSource(Source source);

}
