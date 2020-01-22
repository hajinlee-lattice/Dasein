package com.latticeengines.quartzclient.entitymanager.impl.core;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.quartzclient.dao.JobHistoryDao;
import com.latticeengines.quartzclient.entitymanager.core.BaseJobHistoryEntityMgr;

@Component("baseJobHistoryEntityMgr")
public class BaseJobHistoryEntityMgrImpl extends BaseEntityMgrImpl<JobHistory> implements
        BaseJobHistoryEntityMgr {

    @Inject
    protected JobHistoryDao jobHistoryDao;

    @Override
    public BaseDao<JobHistory> getDao() {
        return jobHistoryDao;
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public JobHistory getJobHistory(String tenantId, String jobName, String triggeredJobHandle) {
        return jobHistoryDao.getJobHistory(tenantId, jobName, triggeredJobHandle);
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRED)
    public void updateJobHistory(JobHistory jobHistory) {
        jobHistoryDao.updateJobHistory(jobHistory);

    }

}
