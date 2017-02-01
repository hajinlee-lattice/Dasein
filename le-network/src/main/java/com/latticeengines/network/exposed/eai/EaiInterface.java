package com.latticeengines.network.exposed.eai;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.eai.EaiJobConfiguration;

public interface EaiInterface {

    AppSubmission submitEaiJob(EaiJobConfiguration eaiJobConfig);
}
