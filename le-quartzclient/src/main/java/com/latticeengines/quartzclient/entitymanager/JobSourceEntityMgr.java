package com.latticeengines.quartzclient.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.quartz.JobSource;

public interface JobSourceEntityMgr extends BaseEntityMgr<JobSource> {

    JobSource getJobSourceType(String tenantId, String jobName);

    void saveJobSource(JobSource jobSource);
}
