package com.latticeengines.network.exposed.dataplatform;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.api.StringList;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.DataProfileConfiguration;
import com.latticeengines.domain.exposed.modeling.DataReviewConfiguration;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;
import com.latticeengines.domain.exposed.modeling.Model;
import com.latticeengines.domain.exposed.modeling.SamplingConfiguration;
import com.latticeengines.domain.exposed.modeling.review.ModelReviewResults;
import com.latticeengines.domain.exposed.modeling.review.RuleRemediationEnablement;

public interface ModelInterface {

    JobStatus getJobStatus(String applicationId);

    AppSubmission loadData(LoadConfiguration config);

    AppSubmission exportData(ExportConfiguration config);

    StringList getFeatures(Model model);

    AppSubmission submit(Model model);

    AppSubmission review(DataReviewConfiguration config);

    AppSubmission profile(DataProfileConfiguration config);

    AppSubmission createSamples(SamplingConfiguration config);

    RuleRemediationEnablement getRuleEnablements(String modelId);

    Boolean setRuleEnablements(String modelId, RuleRemediationEnablement enablement);

    ModelReviewResults getModelReviewResults(String modelId);
}
