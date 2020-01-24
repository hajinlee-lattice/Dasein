package com.latticeengines.datacloud.core.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.datacloud.core.dao.AccountMasterFactDao;
import com.latticeengines.datacloud.core.entitymgr.AccountMasterFactEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterFact;

@Component("accountMasterFactEntityMgr")
public class AccountMasterFactEntityMgrImpl implements AccountMasterFactEntityMgr {

    @Inject
    private AccountMasterFactDao accountMasterFactDao;

    @Override
    @Transactional(value = "propDataManage", readOnly = true, isolation = Isolation.READ_UNCOMMITTED)
    public AccountMasterFact findByDimensions(Long location, Long industry, Long numEmpRange, Long revRange,
            Long numLocRange, Long category) {
        return accountMasterFactDao.findByDimensions(location, industry, numEmpRange, revRange, numLocRange, category);
    }

}
