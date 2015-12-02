package com.latticeengines.pls.entitymanager.impl.microservice;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;

public interface JobProxy {

    JobStatus getJobStatus(String applicationId);
}
