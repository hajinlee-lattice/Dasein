package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dante.dao.DanteTalkingPointDao;
import com.latticeengines.dante.entitymgr.DanteTalkingPointEntityMgr;
import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.dantedb.exposed.entitymgr.impl.BaseDanteEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.DanteTalkingPoint;

@Component("danteTalkingPointEntityMgr")
public class DanteTalkingPointEntityMgrImpl extends BaseDanteEntityMgrImpl<DanteTalkingPoint>
        implements DanteTalkingPointEntityMgr {

    private static final Logger log = Logger.getLogger(DanteTalkingPointEntityMgrImpl.class);

    @Autowired
    private DanteTalkingPointDao danteTalkingPointDao;

    @Override
    public BaseDanteDao<DanteTalkingPoint> getDao() {
        return danteTalkingPointDao;
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public DanteTalkingPoint findByExternalID(String externalId) {
        return danteTalkingPointDao.findByExternalID(externalId);
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<DanteTalkingPoint> findAllByPlayID(String playID) {
        return danteTalkingPointDao.findAllByPlayID(playID);
    }

}
