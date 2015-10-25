package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.ThrottleConfiguration;

public interface ModelingService {

    List<ApplicationId> submitModel(Model model);

    void throttle(ThrottleConfiguration config);

    void resetThrottle();

    ApplicationId createSamples(SamplingConfiguration config);

    ApplicationId loadData(LoadConfiguration config);

    JobStatus getJobStatus(String applicationId);

    List<String> getFeatures(Model model, boolean depivoted);

    ApplicationId profileData(DataProfileConfiguration dataProfileConfig);

    ApplicationId exportData(ExportConfiguration config);
}
