package com.latticeengines.quartzclient.entitymanager.impl.core;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.quartzclient.dao.JobHistoryDao;
import com.latticeengines.quartzclient.entitymanager.core.BaseJobHistoryEntityMgr;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

@Component("baseJobHistoryEntityMgr")
public class BaseJobHistoryEntityMgrImpl extends BaseEntityMgrImpl<JobHistory> implements
        BaseJobHistoryEntityMgr {

    @Autowired
    protected JobHistoryDao jobHistoryDao;

    @Override
    public BaseDao<JobHistory> getDao() {
        return jobHistoryDao;
    }

    @Override
    @Transactional(value = "transactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public JobHistory getJobHistory(String tenantId, String jobName, String triggeredJobHandle) {
        return jobHistoryDao.getJobHistory(tenantId, jobName, triggeredJobHandle);
    }

    @Override
    @Transactional(value = "transactionManager", propagation = Propagation.REQUIRED)
    public void updateJobHistory(JobHistory jobHistory) {
        jobHistoryDao.updateJobHistory(jobHistory);

    }

}
