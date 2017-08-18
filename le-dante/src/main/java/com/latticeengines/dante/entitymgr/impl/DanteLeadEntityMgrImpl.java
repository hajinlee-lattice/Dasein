package com.latticeengines.dante.entitymgr.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dante.dao.DanteLeadDao;
import com.latticeengines.dante.entitymgr.DanteLeadEntityMgr;
import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.dantedb.exposed.entitymgr.impl.BaseDanteEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.DanteLead;

@Component("danteAccountEntityMgr")
public class DanteLeadEntityMgrImpl extends BaseDanteEntityMgrImpl<DanteLead> implements DanteLeadEntityMgr {
    private static final Logger log = LoggerFactory.getLogger(DanteLeadEntityMgrImpl.class);

    @Autowired
    private DanteLeadDao danteLeadDao;

    @Override
    public BaseDanteDao<DanteLead> getDao() {
        return danteLeadDao;
    }

}
