package com.latticeengines.quartzclient.entitymanager.impl;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.quartz.JobSource;
import com.latticeengines.quartzclient.dao.JobSourceDao;
import com.latticeengines.quartzclient.entitymanager.JobSourceEntityMgr;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component("jobSourceEntityMgr")
public class JobSourceEntityMgrImpl extends BaseEntityMgrImpl<JobSource> implements JobSourceEntityMgr {

    @Autowired
    private JobSourceDao jobSourceDao;

    @Override
    public BaseDao<JobSource> getDao() {
        return jobSourceDao;
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public JobSource getJobSourceType(String tenantId, String jobName) {
        return jobSourceDao.getJobSourceType(tenantId, jobName);
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRED)
    public void saveJobSource(JobSource jobSource) {
        jobSourceDao.saveJobSource(jobSource);
    }
}
