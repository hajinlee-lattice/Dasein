package com.latticeengines.dataplatform.entitymanager.impl;

import com.latticeengines.dataplatform.dao.ModelDownloadFlagDao;
import com.latticeengines.dataplatform.entitymanager.ModelDownloadFlagEntityMgr;
import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;

@Component("modelDownloadFlagEntityMgr")
public class ModelDownloadFlagEntityMgrImpl extends BaseEntityMgrImpl<ModelSummaryDownloadFlag> implements
        ModelDownloadFlagEntityMgr {
    @Autowired
    private ModelDownloadFlagDao modelDownloadFlagDao;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void addDownloadFlag(String tenantId) {
        ModelSummaryDownloadFlag flag = new ModelSummaryDownloadFlag();
        flag.setTenantId(tenantId);
        flag.setMarkTime(new Date(System.currentTimeMillis()));
        modelDownloadFlagDao.create(flag);
    }

    @Override
    public BaseDao<ModelSummaryDownloadFlag> getDao() {
        return modelDownloadFlagDao;
    }
}
