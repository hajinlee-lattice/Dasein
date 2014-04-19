package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.exposed.domain.JobStatus;
import com.latticeengines.dataplatform.exposed.domain.LoadConfiguration;
import com.latticeengines.dataplatform.exposed.domain.Model;
import com.latticeengines.dataplatform.exposed.domain.SamplingConfiguration;
import com.latticeengines.dataplatform.exposed.domain.ThrottleConfiguration;

public interface ModelingService {

    List<ApplicationId> submitModel(Model model);

    void throttle(ThrottleConfiguration config);

    ApplicationId createSamples(SamplingConfiguration config);

    ApplicationId createFeatures(Model model);

    ApplicationId loadData(LoadConfiguration config);
    
    JobStatus getJobStatus(String applicationId);
}
