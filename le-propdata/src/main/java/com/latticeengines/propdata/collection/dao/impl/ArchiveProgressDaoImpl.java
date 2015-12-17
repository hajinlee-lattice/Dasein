package com.latticeengines.propdata.collection.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.propdata.collection.dao.ArchiveProgressDao;

@Component("archiveProgressDao")
public class ArchiveProgressDaoImpl extends ProgressDaoImplBase<ArchiveProgress>
        implements ArchiveProgressDao {

    @Override
    protected Class<ArchiveProgress> getEntityClass() {
        return ArchiveProgress.class;
    }

}
