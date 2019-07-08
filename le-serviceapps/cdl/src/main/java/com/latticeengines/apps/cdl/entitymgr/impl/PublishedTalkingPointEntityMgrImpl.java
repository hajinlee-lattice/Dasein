package com.latticeengines.apps.cdl.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.cdl.dao.PublishedTalkingPointDao;
import com.latticeengines.apps.cdl.entitymgr.PublishedTalkingPointEntityMgr;
import com.latticeengines.apps.cdl.repository.PublishedTalkingPointRepository;
import com.latticeengines.apps.cdl.repository.reader.PublishedTalkingPointReaderRepository;
import com.latticeengines.apps.cdl.repository.writer.PublishedTalkingPointWriterRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseReadWriteRepoEntityMgrImpl;
import com.latticeengines.domain.exposed.cdl.PublishedTalkingPoint;
import com.latticeengines.domain.exposed.cdl.TalkingPointDTO;

@Controller("publishedTalkingPointEntityMgr")
public class PublishedTalkingPointEntityMgrImpl
        extends BaseReadWriteRepoEntityMgrImpl<PublishedTalkingPointRepository, PublishedTalkingPoint, Long>
        implements PublishedTalkingPointEntityMgr {
    @SuppressWarnings("unused")
    private static final Logger log = LoggerFactory.getLogger(TalkingPointEntityMgrImpl.class);

    @Autowired
    private PublishedTalkingPointDao publishedTalkingPointDao;

    @Inject
    private PublishedTalkingPointEntityMgrImpl _self;

    @Inject
    private PublishedTalkingPointWriterRepository writerRepository;

    @Inject
    private PublishedTalkingPointReaderRepository readerRepository;

    @Override
    protected PublishedTalkingPointRepository getReaderRepo() {
        return readerRepository;
    }

    @Override
    protected PublishedTalkingPointRepository getWriterRepo() {
        return writerRepository;
    }

    @Override
    protected BaseReadWriteRepoEntityMgrImpl<PublishedTalkingPointRepository, PublishedTalkingPoint, Long> getSelf() {
        return _self;
    }

    @Override
    public BaseDao<PublishedTalkingPoint> getDao() {
        return publishedTalkingPointDao;
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public PublishedTalkingPoint findByName(String name) {
        return publishedTalkingPointDao.findByField("name", name);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<PublishedTalkingPoint> findAllByPlayName(String playName) {
        return publishedTalkingPointDao.findAllByPlayName(playName);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<TalkingPointDTO> findAllByTenantPid(Long tenantPid) {
        return readerRepository.findAllByTenantPid(tenantPid);
    }

}
