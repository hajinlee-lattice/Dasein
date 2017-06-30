package com.latticeengines.dataplatform.service.modeling;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.modeling.ModelingJob;
import com.latticeengines.yarn.exposed.service.JobService;

public interface ModelingJobService extends JobService {

    ApplicationId resubmitPreemptedJob(ModelingJob modelingJob);

    ApplicationId submitJob(ModelingJob modelingJob);

}
