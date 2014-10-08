package com.latticeengines.dataplatform.service.jetty;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.jetty.JettyJob;

public interface JettyJobService extends JobService {

    ApplicationId submitJob(JettyJob jettyJob);

}
