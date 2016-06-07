package com.latticeengines.quartzclient.entitymanager.core;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.quartz.JobHistory;

public interface BaseJobHistoryEntityMgr extends BaseEntityMgr<JobHistory> {

    JobHistory getJobHistory(String tenantId, String jobName, String triggeredJobHandle);

    public void updateJobHistory(JobHistory jobHistory);

}
