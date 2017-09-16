package com.latticeengines.pls.entitymanager.impl;

import java.util.Date;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;
import com.latticeengines.pls.dao.ModelSummaryDownloadFlagDao;
import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;

@Component("modelSummaryDownloadFlagEntityMgr")
public class ModelSummaryDownloadFlagEntityMgrImpl extends BaseEntityMgrImpl<ModelSummaryDownloadFlag> implements
        ModelSummaryDownloadFlagEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(ModelSummaryDownloadFlagEntityMgrImpl.class);

    @Autowired
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
    @Transactional(propagation = Propagation.REQUIRED)
    public void addDownloadFlag(String tenantId) {
        ModelSummaryDownloadFlag flag = new ModelSummaryDownloadFlag();
        flag.setTenantId(tenantId);
        flag.setMarkTime(new Date(System.currentTimeMillis()));
        log.info(String.format("Set model summary download flag for tenant %s by entityMgr.", tenantId));
        modelSummaryDownloadFlagDao.create(flag);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void removeDownloadedFlag(long timeTicks) {
        modelSummaryDownloadFlagDao.deleteOldFlags(timeTicks);
    }


}
