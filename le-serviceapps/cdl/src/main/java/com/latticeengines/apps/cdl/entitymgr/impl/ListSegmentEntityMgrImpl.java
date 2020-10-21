package com.latticeengines.apps.cdl.entitymgr.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Controller;

import com.latticeengines.apps.cdl.dao.ListSegmentDao;
import com.latticeengines.apps.cdl.entitymgr.ListSegmentEntityMgr;
import com.latticeengines.apps.cdl.repository.ListSegmentRepository;
import com.latticeengines.apps.cdl.repository.reader.ListSegmentReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.ListSegmentWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.metadata.ListSegment;

@Controller("talkingPointEntityMgr")
public class ListSegmentEntityMgrImpl extends
        BaseReadWriteRepoEntityMgrImpl<ListSegmentRepository, ListSegment, Long> implements ListSegmentEntityMgr {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(ListSegmentEntityMgrImpl.class);

    @Inject
    private ListSegmentEntityMgrImpl _self;

    @Inject
    private ListSegmentDao listSegmentDao;

    @Inject
    private ListSegmentWriterRepository writerRepository;

    @Inject
    private ListSegmentReaderRepository readerRepository;

    @Override
    public BaseDao<ListSegment> getDao() {
        return listSegmentDao;
    }

    @Override
    protected ListSegmentRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected ListSegmentRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<ListSegmentRepository, ListSegment, Long> getSelf() {
        return _self;
    }

}
