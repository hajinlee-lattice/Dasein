package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.apache.log4j.Logger;
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

    private static final Logger log = Logger.getLogger(TalkingPointEntityMgrImpl.class);

    @Autowired
    private TalkingPointDao talkingPointDao;

    @Override
    public BaseDao<TalkingPoint> getDao() {
        return talkingPointDao;
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<TalkingPoint> findAllByPlayID(Long playId) {
        return talkingPointDao.findAllByPlayID(playId);
    }
}
