package com.latticeengines.apps.dcp.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.apps.dcp.dao.ProjectSystemLinkDao;
import com.latticeengines.apps.dcp.entitymgr.ProjectSystemLinkEntityMgr;
import com.latticeengines.apps.dcp.repository.ProjectSystemLinkRepository;
import com.latticeengines.apps.dcp.repository.reader.ProjectSystemLinkReaderRepository;
import com.latticeengines.apps.dcp.repository.writer.ProjectSystemLinkWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.dcp.ProjectSystemLink;

@Component("projectSystemLinkEntityMgr")
public class ProjectSystemLinkEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<ProjectSystemLinkRepository, ProjectSystemLink, Long>
        implements ProjectSystemLinkEntityMgr {

    @Inject
    private ProjectSystemLinkEntityMgrImpl _self;

    @Inject
    private ProjectSystemLinkReaderRepository readerRepository;

    @Inject
    private ProjectSystemLinkWriterRepository writerRepository;

    @Inject
    private ProjectSystemLinkDao projectSystemLinkDao;

    @Override
    protected ProjectSystemLinkRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected ProjectSystemLinkRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<ProjectSystemLinkRepository, ProjectSystemLink, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<ProjectSystemLink> getDao() {
        return projectSystemLinkDao;
    }
}
