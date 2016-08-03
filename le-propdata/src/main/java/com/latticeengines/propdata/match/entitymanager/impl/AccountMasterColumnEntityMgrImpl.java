package com.latticeengines.propdata.match.entitymanager.impl;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.propdata.manage.AccountMasterColumn;
import com.latticeengines.propdata.match.dao.AccountMasterColumnDao;
import com.latticeengines.propdata.match.entitymanager.MetadataColumnEntityMgr;

@Component("accountMasterColumnEntityMgr")
public class AccountMasterColumnEntityMgrImpl implements MetadataColumnEntityMgr<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnDao")
    private AccountMasterColumnDao accountMasterColumnDao;

    protected AccountMasterColumnDao getExternalColumnDao() {
        return accountMasterColumnDao;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AccountMasterColumn> findByTag(String tag) {
        return accountMasterColumnDao.findByTag(tag);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AccountMasterColumn> findAll() {
        return accountMasterColumnDao.findAll();
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AccountMasterColumn findById(String amColumnId) {
        return accountMasterColumnDao.findByField("AMColumnID", amColumnId);
    }
}
