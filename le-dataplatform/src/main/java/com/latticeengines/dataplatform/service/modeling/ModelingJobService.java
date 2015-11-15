package com.latticeengines.dataplatform.service.modeling;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.modeling.ModelingJob;

public interface ModelingJobService extends JobService {

    ApplicationId resubmitPreemptedJob(ModelingJob modelingJob);

    ApplicationId submitJob(ModelingJob modelingJob);

}
