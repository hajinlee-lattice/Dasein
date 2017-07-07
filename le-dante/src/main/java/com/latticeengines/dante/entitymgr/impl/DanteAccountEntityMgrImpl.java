package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dante.dao.DanteAccountDao;
import com.latticeengines.dante.entitymgr.DanteAccountEntityMgr;
import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.dantedb.exposed.entitymgr.impl.BaseDanteEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.DanteAccount;

@Component("danteAccountEntityMgr")
public class DanteAccountEntityMgrImpl extends BaseDanteEntityMgrImpl<DanteAccount> implements DanteAccountEntityMgr {
    private static final Logger log = Logger.getLogger(DanteAccountEntityMgrImpl.class);

    @Autowired
    private DanteAccountDao danteAccountDao;

    @Override
    public BaseDanteDao<DanteAccount> getDao() {
        return danteAccountDao;
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<DanteAccount> getAccounts(int count, String customerID) {
        return danteAccountDao.getAccounts(count, customerID);
    }
}
