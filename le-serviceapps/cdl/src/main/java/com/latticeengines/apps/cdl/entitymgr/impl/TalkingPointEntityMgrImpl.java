package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.TalkingPointDao;
import com.latticeengines.apps.cdl.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.apps.cdl.repository.TalkingPointRepository;
import com.latticeengines.apps.cdl.repository.reader.TalkingPointReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.TalkingPointWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.TalkingPoint;

@Controller("talkingPointEntityMgr")
public class TalkingPointEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<TalkingPointRepository, TalkingPoint, Long>
        implements TalkingPointEntityMgr {

    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TalkingPointEntityMgrImpl.class);

    @Inject
    private TalkingPointEntityMgrImpl _self;

    @Autowired
    private TalkingPointDao talkingPointDao;

    @Inject
    private TalkingPointWriterRepository writerRepository;

    @Inject
    private TalkingPointReaderRepository readerRepository;

    @Override
    public BaseDao<TalkingPoint> getDao() {
        return talkingPointDao;
    }

    @Override
    protected TalkingPointRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected TalkingPointRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<TalkingPointRepository, TalkingPoint, Long> getSelf() {
        return _self;
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<TalkingPoint> findAllByPlayName(String playName) {
        return talkingPointDao.findAllByPlayName(playName);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public TalkingPoint findByName(String name) {
        return talkingPointDao.findByField("name", name);
    }
}
