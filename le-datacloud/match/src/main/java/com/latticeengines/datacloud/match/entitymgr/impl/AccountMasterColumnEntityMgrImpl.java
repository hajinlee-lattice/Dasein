package com.latticeengines.datacloud.match.entitymgr.impl;

import java.util.List;

import javax.annotation.Resource;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.google.common.annotations.VisibleForTesting;
import com.latticeengines.datacloud.match.dao.AccountMasterColumnDao;
import com.latticeengines.datacloud.match.entitymgr.MetadataColumnEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.AccountMasterColumn;

@Component("accountMasterColumnEntityMgr")
public class AccountMasterColumnEntityMgrImpl
        implements MetadataColumnEntityMgr<AccountMasterColumn> {

    @Resource(name = "accountMasterColumnDao")
    private AccountMasterColumnDao accountMasterColumnDao;

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @VisibleForTesting
    public void create(AccountMasterColumn accountMasterColumn) {
        accountMasterColumnDao.create(accountMasterColumn);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    @VisibleForTesting
    public void deleteByColumnIdAndDataCloudVersion(String columnId, String dataCloudVersion) {
        accountMasterColumnDao.deleteByIdByDataCloudVersion(columnId, dataCloudVersion);
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
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public AccountMasterColumn findById(String amColumnId, String dataCloudVersion) {
        return accountMasterColumnDao.findById(amColumnId, dataCloudVersion);
    }

    @Override
    @Transactional(value = "propDataManage", propagation = Propagation.REQUIRED)
    public void updateMetadataColumns(String dataCloudVersion,
            List<AccountMasterColumn> metadataColumns) {
        for (AccountMasterColumn metadataColumn : metadataColumns) {
            accountMasterColumnDao.update(metadataColumn);
        }
    }

}
