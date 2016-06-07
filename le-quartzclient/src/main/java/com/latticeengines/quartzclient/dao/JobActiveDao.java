package com.latticeengines.quartzclient.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.quartz.JobActive;

public interface JobActiveDao extends BaseDao<JobActive> {

    boolean getJobActive(String jobName, String tenantId);

}
