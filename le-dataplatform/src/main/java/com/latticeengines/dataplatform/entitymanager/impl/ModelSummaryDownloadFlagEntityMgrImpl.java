package com.latticeengines.dataplatform.entitymanager.impl;

import com.latticeengines.dataplatform.dao.ModelSummaryDownloadFlagDao;
import com.latticeengines.dataplatform.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

@Component("modelSummaryDownloadFlagEntityMgr")
public class ModelSummaryDownloadFlagEntityMgrImpl extends BaseEntityMgrImpl<ModelSummaryDownloadFlag> implements
        ModelSummaryDownloadFlagEntityMgr {
    @Autowired
    private ModelSummaryDownloadFlagDao modelSummaryDownloadFlagDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void addDownloadFlag(String tenantId) {
        ModelSummaryDownloadFlag flag = new ModelSummaryDownloadFlag();
        flag.setTenantId(tenantId);
        flag.setMarkTime(new Date(System.currentTimeMillis()));
        flag.setDownloaded(false);
        modelSummaryDownloadFlagDao.create(flag);
    }

    @Override
    public BaseDao<ModelSummaryDownloadFlag> getDao() {
        return modelSummaryDownloadFlagDao;
    }
}
