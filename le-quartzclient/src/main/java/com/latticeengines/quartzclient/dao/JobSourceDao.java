package com.latticeengines.quartzclient.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.quartz.JobSource;

public interface JobSourceDao extends BaseDao<JobSource> {

    JobSource getJobSourceType(String tenantId, String jobName);

    void saveJobSource(JobSource jobSource);
}
