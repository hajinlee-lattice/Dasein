package com.latticeengines.dataplatform.exposed.service;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.EventCounterConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelReviewConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public interface ModelingService {

    List<ApplicationId> submitModel(Model model);

    ApplicationId createSamples(SamplingConfiguration config);

    ApplicationId createEventCounter(EventCounterConfiguration config);

    JobStatus getJobStatus(String applicationId);

    List<String> getFeatures(Model model, boolean depivoted);

    ApplicationId profileData(DataProfileConfiguration dataProfileConfig);

    ApplicationId reviewData(ModelReviewConfiguration config);

    Model getModel(String Id);
}
