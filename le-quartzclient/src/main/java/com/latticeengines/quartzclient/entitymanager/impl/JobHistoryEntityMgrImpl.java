package com.latticeengines.quartzclient.entitymanager.impl;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.quartzclient.entitymanager.JobHistoryEntityMgr;
import com.latticeengines.quartzclient.entitymanager.impl.core.BaseJobHistoryEntityMgrImpl;

@Component("jobHistoryEntityMgr")
public class JobHistoryEntityMgrImpl extends BaseJobHistoryEntityMgrImpl implements
        JobHistoryEntityMgr {

    @Override
    public List<JobHistory> getJobHistory(String tenantId, String jobName) {
        return jobHistoryDao.getJobHistory(tenantId, jobName);
    }

    @Override
    public void saveJobHistory(JobHistory jobHistory) {
        jobHistoryDao.saveJobHistory(jobHistory);
    }

    @Override
    public JobHistory getRecentUnfinishedJobHistory(String tenantId, String jobName) {
        return jobHistoryDao.getRecentUnfinishedJobHistory(tenantId, jobName);
    }

}
