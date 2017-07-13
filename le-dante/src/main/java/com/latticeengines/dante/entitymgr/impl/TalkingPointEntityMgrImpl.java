package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dante.dao.TalkingPointDao;
import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.TalkingPoint;

@Controller("talkingPointEntityMgr")
public class TalkingPointEntityMgrImpl extends BaseEntityMgrImpl<TalkingPoint> implements TalkingPointEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(TalkingPointEntityMgrImpl.class);

    @Autowired
    private TalkingPointDao talkingPointDao;

    @Override
    public BaseDao<TalkingPoint> getDao() {
        return talkingPointDao;
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
