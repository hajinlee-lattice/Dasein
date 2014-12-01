package com.latticeengines.dataplatform.service.eai;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.eai.EaiJob;

public interface EaiJobService extends JobService {

    ApplicationId submitJob(EaiJob eaiJob);

}
