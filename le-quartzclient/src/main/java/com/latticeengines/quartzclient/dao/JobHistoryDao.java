package com.latticeengines.quartzclient.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.quartz.JobHistory;

public interface JobHistoryDao extends BaseDao<JobHistory> {
    List<JobHistory> getJobHistory(String tenantId, String jobName);

    JobHistory getRecentUnfinishedJobHistory(String tenantId, String jobName);

    JobHistory getJobHistory(String tenantId, String jobName, String triggeredJobHandle);

    void updateJobHistory(JobHistory jobHistory);

    void saveJobHistory(JobHistory jobHistory);

}
