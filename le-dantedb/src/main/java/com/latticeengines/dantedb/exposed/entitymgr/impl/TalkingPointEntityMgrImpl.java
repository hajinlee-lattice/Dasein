package com.latticeengines.dantedb.exposed.entitymgr.impl;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dantedb.exposed.dao.DanteTalkingPointsDao;
import com.latticeengines.dantedb.exposed.entitymgr.TalkingPointEntityMgr;
import com.latticeengines.domain.exposed.dantetalkingpoints.DanteTalkingPoint;

@Component("talkingPointEntityMgr")
public class TalkingPointEntityMgrImpl implements TalkingPointEntityMgr {

    @Autowired
    DanteTalkingPointsDao dao;

    @Transactional(value = "danteDataSource")
    public void create(DanteTalkingPoint dtp) {
        dao.create(dtp);
    }

    @Transactional(value = "danteDataSource")
    public void delete(DanteTalkingPoint dtp) {
        dao.delete(dtp);
    }

    @Transactional(value = "danteDataSource", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public DanteTalkingPoint findByExternalID(String externalId) {
        return dao.findByExternalID(externalId);
    }

}
