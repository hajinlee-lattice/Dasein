package com.latticeengines.network.exposed.dataplatform;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.ModelReviewConfiguration;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modelreview.ModelReviewData;

public interface ModelInterface {

    JobStatus getJobStatus(String applicationId);

    AppSubmission loadData(LoadConfiguration config);

    AppSubmission exportData(ExportConfiguration config);

    StringList getFeatures(Model model);

    AppSubmission submit(Model model);

    AppSubmission review(ModelReviewConfiguration config);

    AppSubmission profile(DataProfileConfiguration config);

    AppSubmission createSamples(SamplingConfiguration config);

    ModelReviewData getReviewData(String modelId);
}
