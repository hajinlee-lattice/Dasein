package com.latticeengines.network.exposed.dataplatform;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.EventCounterConfiguration;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelReviewConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;

public interface ModelInterface {

    JobStatus getJobStatus(String applicationId);

    AppSubmission loadData(LoadConfiguration config);

    AppSubmission exportData(ExportConfiguration config);

    StringList getFeatures(Model model);

    Model getModel(String Id);

    AppSubmission submit(Model model);

    AppSubmission review(ModelReviewConfiguration config);

    AppSubmission profile(DataProfileConfiguration config);

    AppSubmission createEventCounter(EventCounterConfiguration config);

    AppSubmission createSamples(SamplingConfiguration config);

}
