package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dante.dao.PublishedTalkingPointDao;
import com.latticeengines.dante.entitymgr.PublishedTalkingPointEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.multitenant.PublishedTalkingPoint;

@Controller("publishedTalkingPointEntityMgr")
public class PublishedTalkingPointEntityMgrImpl extends BaseEntityMgrImpl<PublishedTalkingPoint>
        implements PublishedTalkingPointEntityMgr {
    private static final Logger log = LoggerFactory.getLogger(TalkingPointEntityMgrImpl.class);

    @Autowired
    private PublishedTalkingPointDao publishedTalkingPointDao;

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

}
