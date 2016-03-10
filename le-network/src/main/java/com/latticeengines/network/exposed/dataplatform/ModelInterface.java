package com.latticeengines.network.exposed.dataplatform;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;

public interface ModelInterface {

    JobStatus getJobStatus(String applicationId);
}
