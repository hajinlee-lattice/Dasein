package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dante.dao.DanteAccountDao;
import com.latticeengines.dante.entitymgr.DanteAccountEntityMgr;
import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.dantedb.exposed.entitymgr.impl.BaseDanteEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.DanteAccount;

/**
 * Retrieves Accounts for Talking Point UI from Dante database. Starting from
 * M16 we should not use this. Objectapi should be used instead
 */
@Deprecated
@Component("danteAccountEntityMgr")
public class DanteAccountEntityMgrImpl extends BaseDanteEntityMgrImpl<DanteAccount> implements DanteAccountEntityMgr {
    private static final Logger log = LoggerFactory.getLogger(DanteAccountEntityMgrImpl.class);

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
