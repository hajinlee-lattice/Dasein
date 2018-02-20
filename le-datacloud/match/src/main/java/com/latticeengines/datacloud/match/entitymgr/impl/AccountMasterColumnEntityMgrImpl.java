package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;
import javax.inject.Inject;

import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Sort;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.match.dao.AccountMasterColumnDao;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.datacloud.match.repository.AccountMasterColumnRepository;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrRepositoryImpl;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

@Component("accountMasterColumnEntityMgr")
public class AccountMasterColumnEntityMgrImpl
        extends BaseEntityMgrRepositoryImpl<AccountMasterColumn, Long>
        implements MetadataColumnEntityMgr<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnDao")
    private AccountMasterColumnDao accountMasterColumnDao;

    @Inject
    private AccountMasterColumnRepository repository;

    @Override
    public BaseDao<AccountMasterColumn> getDao() {
        return accountMasterColumnDao;
    }

    @Override
    public BaseJpaRepository<AccountMasterColumn, Long> getRepository() {
        return repository;
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @VisibleForTesting
    public void create(AccountMasterColumn accountMasterColumn) {
        accountMasterColumnDao.create(accountMasterColumn);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AccountMasterColumn> findByTag(String tag, String dataCloudVersion) {
        return accountMasterColumnDao.findByTag(tag, dataCloudVersion);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<AccountMasterColumn> findAll(String dataCloudVersion) {
        return accountMasterColumnDao.findAllByField("dataCloudVersion", dataCloudVersion);
    }

    @Override
    public List<AccountMasterColumn> findByPage(String dataCloudVersion, int page, int pageSize) {
        PageRequest pageRequest = PageRequest.of(page, pageSize, Sort.by("amColumnId"));
        return repository.findByDataCloudVersion(dataCloudVersion, pageRequest);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AccountMasterColumn findById(String amColumnId, String dataCloudVersion) {
        return accountMasterColumnDao.findById(amColumnId, dataCloudVersion);
    }

    @Override
    public Long count(String dataCloudVersion) {
        return repository.countByDataCloudVersion(dataCloudVersion);
    }

}
