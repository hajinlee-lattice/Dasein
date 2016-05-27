package com.latticeengines.proxy.exposed.scoring;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.network.exposed.scoring.ScoringInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("scoringProxy")
public class ScoringProxy extends BaseRestApiProxy implements ScoringInterface {

    public ScoringProxy() {
        super("scoring/scoringjobs");
    }

    @Override
    public AppSubmission createScoringJob(ScoringConfiguration scoringConfig) {
        String url = constructUrl();
        return post("createScoringJob", url, scoringConfig, AppSubmission.class);
    }

    @Override
    public JobStatus getImportDataJobStatus(String applicationId) {
        String url = constructUrl("{applicationId}", applicationId);
        return get("getImportDataJobStatus", url, JobStatus.class);
    }

    @Override
    public ApplicationId submitBulkScoreJob(RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        String url = constructUrl("/rtsbulkscore");
        return post("createRTSBulkScoringJob", url, rtsBulkScoringConfig, ApplicationId.class);
    }

}
