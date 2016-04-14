package com.latticeengines.network.exposed.dataplatform;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.modeling.ExportConfiguration;
import com.latticeengines.domain.exposed.modeling.LoadConfiguration;

public interface ModelInterface {

    JobStatus getJobStatus(String applicationId);

    AppSubmission loadData(LoadConfiguration config);

    AppSubmission exportData(ExportConfiguration config);
}
