package com.latticeengines.network.exposed.scoring;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;

public interface ScoringInterface {

    AppSubmission createScoringJob(ScoringConfiguration scoringConfig);

    JobStatus getImportDataJobStatus(String applicationId);

    AppSubmission submitBulkScoreJob(RTSBulkScoringConfiguration rtsBulkScoringConfig);

}
