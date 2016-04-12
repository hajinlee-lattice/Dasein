package com.latticeengines.quartz.entitymanager;

import java.util.List;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.quartz.JobHistory;

public interface JobHistoryEntityMgr extends BaseEntityMgr<JobHistory> {

    List<JobHistory> getJobHistory(String tenantId, String jobName);

    void saveJobHistory(JobHistory jobHistory);

}
