package com.latticeengines.datacloud.collection.dao.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.dao.ArchiveProgressDao;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;

@Component("archiveProgressDao")
public class ArchiveProgressDaoImpl extends ProgressDaoImplBase<ArchiveProgress>
        implements ArchiveProgressDao {

    @Override
    protected Class<ArchiveProgress> getEntityClass() {
        return ArchiveProgress.class;
    }

}
