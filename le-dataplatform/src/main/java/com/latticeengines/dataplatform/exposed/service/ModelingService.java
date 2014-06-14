package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.dataplatform.LoadConfiguration;
import com.latticeengines.domain.exposed.dataplatform.Model;
import com.latticeengines.domain.exposed.dataplatform.SamplingConfiguration;
import com.latticeengines.domain.exposed.dataplatform.ThrottleConfiguration;

public interface ModelingService {

    List<ApplicationId> submitModel(Model model);

    void throttle(ThrottleConfiguration config);

    void resetThrottle();
    
    ApplicationId createSamples(SamplingConfiguration config);

    ApplicationId createFeatures(Model model);

    List<ApplicationId> loadData(LoadConfiguration config);
    
    JobStatus getJobStatus(String applicationId);

    List<String> getFeatures(Model model, boolean depivoted);
}
