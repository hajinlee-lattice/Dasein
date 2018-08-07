package com.latticeengines.apps.lp.entitymgr.impl;

import java.util.Date;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.apps.lp.dao.ModelSummaryDownloadFlagDao;
import com.latticeengines.apps.lp.entitymgr.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;

@Component("modelSummaryDownloadFlagEntityMgr")
public class ModelSummaryDownloadFlagEntityMgrImpl extends BaseEntityMgrImpl<ModelSummaryDownloadFlag> implements
        ModelSummaryDownloadFlagEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryDownloadFlagEntityMgrImpl.class);

    @Inject
    private ModelSummaryDownloadFlagDao modelSummaryDownloadFlagDao;

    @Override
    public BaseDao<ModelSummaryDownloadFlag> getDao() {
        return modelSummaryDownloadFlagDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> getWaitingFlags() {
        return modelSummaryDownloadFlagDao.getWaitingFlags();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<String> getExcludeFlags() {
        return modelSummaryDownloadFlagDao.getExcludeFlags();
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void addDownloadFlag(String tenantId) {
        ModelSummaryDownloadFlag flag = new ModelSummaryDownloadFlag();
        flag.setTenantId(tenantId);
        flag.setMarkTime(new Date(System.currentTimeMillis()));
        log.info(String.format("Set model summary download flag for tenant %s by entityMgr.", tenantId));
        modelSummaryDownloadFlagDao.create(flag);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void addExcludeFlag(String tenantId) {
        ModelSummaryDownloadFlag flag = new ModelSummaryDownloadFlag();
        flag.setExcludeTenantID(tenantId);
        flag.setMarkTime(new Date(System.currentTimeMillis()));
        log.info(String.format("Set model summary exclude flag for tenant %s by entityMgr.", tenantId));
        modelSummaryDownloadFlagDao.create(flag);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void removeDownloadedFlag(long timeTicks) {
        modelSummaryDownloadFlagDao.deleteOldFlags(timeTicks);
    }

    @Override
    @Transactional(transactionManager = "transactionManager", propagation = Propagation.REQUIRED)
    public void removeExcludeFlag(String tenantId) { modelSummaryDownloadFlagDao.deleteExcludeFlag(tenantId); }
}
