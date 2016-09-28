package com.latticeengines.pls.entitymanager.impl;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.pls.ModelSummaryDownloadFlag;
import com.latticeengines.pls.dao.ModelSummaryDownloadFlagDao;
import com.latticeengines.pls.entitymanager.ModelSummaryDownloadFlagEntityMgr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.Date;
import java.util.List;

@Component("modelSummaryDownloadFlagEntityMgr")
public class ModelSummaryDownloadFlagEntityMgrImpl extends BaseEntityMgrImpl<ModelSummaryDownloadFlag> implements
        ModelSummaryDownloadFlagEntityMgr {

    @Autowired
    private ModelSummaryDownloadFlagDao modelSummaryDownloadFlagDao;

    @Override
    public BaseDao<ModelSummaryDownloadFlag> getDao() {
        return modelSummaryDownloadFlagDao;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummaryDownloadFlag> getDownloadedFlags() {
        return modelSummaryDownloadFlagDao.getDownloadedFlags();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<ModelSummaryDownloadFlag> getWaitingFlags() {
        return modelSummaryDownloadFlagDao.getWaitingFlags();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void addDownloadFlag(String tenantId) {
        ModelSummaryDownloadFlag flag = new ModelSummaryDownloadFlag();
        flag.setTenantId(tenantId);
        flag.setMarkTime(new Date(System.currentTimeMillis()));
        modelSummaryDownloadFlagDao.create(flag);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void removeDownloadedFlag(long timeTicks) {
        List<ModelSummaryDownloadFlag> downloadedFlags = modelSummaryDownloadFlagDao.getDownloadedFlags();
        if (downloadedFlags != null) {
            for (int i = 0; i < downloadedFlags.size(); i++) {
                if (downloadedFlags.get(i).getMarkTime().getTime() < timeTicks) {
                    modelSummaryDownloadFlagDao.delete(downloadedFlags.get(i));
                } else {
                    return;
                }
            }
        }
    }


}
