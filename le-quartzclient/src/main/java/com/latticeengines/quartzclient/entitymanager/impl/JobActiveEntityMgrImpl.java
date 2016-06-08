package com.latticeengines.quartzclient.entitymanager.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.quartz.JobActive;
import com.latticeengines.quartzclient.dao.JobActiveDao;
import com.latticeengines.quartzclient.entitymanager.JobActiveEntityMgr;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component("jobActiveEntityMgr")
public class JobActiveEntityMgrImpl extends BaseEntityMgrImpl<JobActive> implements
        JobActiveEntityMgr {

    @Autowired
    private JobActiveDao jobActiveDao;

    @Override
    public BaseDao<JobActive> getDao() {
        return jobActiveDao;
    }

    @Override
    @Transactional(value = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public boolean getJobActive(String jobName, String tenantId) {
        return jobActiveDao.getJobActive(jobName, tenantId);
    }

}
