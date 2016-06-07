package com.latticeengines.quartzclient.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.quartz.JobActive;

public interface JobActiveEntityMgr extends BaseEntityMgr<JobActive> {

    boolean getJobActive(String jobName, String tenantId);

}
