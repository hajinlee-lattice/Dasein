package com.latticeengines.quartzclient.entitymanager.impl;

import java.util.List;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;
import com.latticeengines.quartzclient.entitymanager.impl.core.BaseJobHistoryEntityMgrImpl;

@Component("jobHistoryEntityMgr")
public class JobHistoryEntityMgrImpl extends BaseJobHistoryEntityMgrImpl implements
        JobHistoryEntityMgr {

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<JobHistory> getJobHistory(String tenantId, String jobName) {
        return jobHistoryDao.getJobHistory(tenantId, jobName);
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRED)
    public void saveJobHistory(JobHistory jobHistory) {
        jobHistoryDao.saveJobHistory(jobHistory);
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRED)
    public void createJobHistory(JobHistory jobHistory) {
        jobHistoryDao.createJobHistory(jobHistory);
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRED)
    public void deleteOldJobHistory(int retainingDays) {
        jobHistoryDao.deleteOldJobHistory(retainingDays);
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public JobHistory getRecentUnfinishedJobHistory(String tenantId, String jobName) {
        return jobHistoryDao.getRecentUnfinishedJobHistory(tenantId, jobName);
    }

    @Override
    @Transactional(value = "qrtzTransactionManager", propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public JobHistory getLastJobHistory(String tenantId, String jobName) {
        return jobHistoryDao.getLastJobHistory(tenantId, jobName);
    }

}
