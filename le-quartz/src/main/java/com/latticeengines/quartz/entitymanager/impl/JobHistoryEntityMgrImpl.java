package com.latticeengines.quartz.entitymanager.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.db.exposed.entitymgr.impl.BaseEntityMgrImpl;
import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.quartz.dao.JobHistoryDao;
import com.latticeengines.quartz.entitymanager.JobHistoryEntityMgr;

@Component("jobHistoryEntityMgr")
public class JobHistoryEntityMgrImpl extends BaseEntityMgrImpl<JobHistory> implements
        JobHistoryEntityMgr {

    @Autowired
    private JobHistoryDao jobHistoryDao;

    @Override
    public BaseDao<JobHistory> getDao() {
        return jobHistoryDao;
    }

    @Override
    public List<JobHistory> getJobHistory(String tenantId, String jobName) {
        return jobHistoryDao.getJobHistory(tenantId, jobName);
    }

    @Override
    public void saveJobHistory(JobHistory jobHistory) {
        jobHistoryDao.saveJobHistory(jobHistory);
    }
}
