package com.latticeengines.dante.entitymgr.impl;

import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.dante.dao.AccountCacheDao;
import com.latticeengines.dante.entitymgr.AccountEntityMgr;
import com.latticeengines.dantedb.exposed.dao.BaseDanteDao;
import com.latticeengines.dantedb.exposed.entitymgr.impl.BaseDanteEntityMgrImpl;
import com.latticeengines.domain.exposed.dante.DanteAccount;

@Component("accountCacheEntityMgr")
public class AccountEntityMgrImpl extends BaseDanteEntityMgrImpl<DanteAccount> implements AccountEntityMgr {
    private static final Logger log = Logger.getLogger(AccountEntityMgrImpl.class);

    @Autowired
    private AccountCacheDao accountCacheDao;

    @Override
    public BaseDanteDao<DanteAccount> getDao() {
        return accountCacheDao;
    }

    @Transactional(readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public List<DanteAccount> getAccounts(int count, String customerID) {
        return accountCacheDao.getAccounts(count, customerID);
    }
}
