package com.latticeengines.quartzclient.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.quartz.JobHistory;
import com.latticeengines.quartzclient.entitymanager.core.BaseJobHistoryEntityMgr;

public interface JobHistoryEntityMgr extends BaseJobHistoryEntityMgr {

    List<JobHistory> getJobHistory(String tenantId, String jobName);

    JobHistory getRecentUnfinishedJobHistory(String tenantId, String jobName);

    JobHistory getLastJobHistory(String tenantId, String jobName);

    void saveJobHistory(JobHistory jobHistory);

    void createJobHistory(JobHistory jobHistory);

    void deleteOldJobHistory(int retainingDays);

}
