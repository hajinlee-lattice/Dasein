package com.latticeengines.quartzclient.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.quartz.JobHistory;

public interface JobHistoryDao extends BaseDao<JobHistory> {
    List<JobHistory> getJobHistory(String tenantId, String jobName);

    JobHistory getRecentUnfinishedJobHistory(String tenantId, String jobName);

    JobHistory getJobHistory(String tenantId, String jobName, String triggeredJobHandle);

    JobHistory getLastJobHistory(String tenantId, String jobName);

    void updateJobHistory(JobHistory jobHistory);

    void saveJobHistory(JobHistory jobHistory);

    void createJobHistory(JobHistory jobHistory);

    void deleteOldJobHistory(int retainingDays);

    void deleteAllJobHistory(String tenantId, String jobName);

}
