package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dante.dao.TalkingPointDao;
import com.latticeengines.dante.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.dantedb.exposed.entitymgr.impl.BaseDanteEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

@Component("talkingPointEntityMgr")
public class TalkingPointEntityMgrImpl extends BaseDanteEntityMgrImpl<DanteTalkingPoint>
        implements TalkingPointEntityMgr {

    @Autowired
    TalkingPointDao talkingPointDao;

    @Override
    public BaseDanteDao<DanteTalkingPoint> getDao() {
        return talkingPointDao;
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public DanteTalkingPoint findByExternalID(String externalId) {
        return talkingPointDao.findByExternalID(externalId);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<DanteTalkingPoint> findAllByPlayID(String playID) {
        return talkingPointDao.findAllByPlayID(playID);
    }

}
