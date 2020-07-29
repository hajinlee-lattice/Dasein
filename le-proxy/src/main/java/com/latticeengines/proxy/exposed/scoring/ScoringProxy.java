package com.latticeengines.proxy.exposed.scoring;

import org.springframework.context.annotation.Scope;
import org.springframework.context.annotation.ScopedProxyMode;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.scoring.RTSBulkScoringConfiguration;
import com.latticeengines.domain.exposed.scoring.ScoringConfiguration;
import com.latticeengines.proxy.exposed.MicroserviceRestApiProxy;

@Component("scoringProxy")
@Scope(proxyMode = ScopedProxyMode.TARGET_CLASS)
public class ScoringProxy extends MicroserviceRestApiProxy {

    public ScoringProxy() {
        super("scoring/scoringjobs");
    }

    public AppSubmission createScoringJob(ScoringConfiguration scoringConfig) {
        String url = constructUrl();
        return post("createScoringJob", url, scoringConfig, AppSubmission.class);
    }

    public JobStatus getImportDataJobStatus(String applicationId) {
        String url = constructUrl("{applicationId}", applicationId);
        return get("getImportDataJobStatus", url, JobStatus.class);
    }

    public AppSubmission submitBulkScoreJob(RTSBulkScoringConfiguration rtsBulkScoringConfig) {
        String url = constructUrl("/rtsbulkscore");
        return post("createRTSBulkScoringJob", url, rtsBulkScoringConfig, AppSubmission.class);
    }

}
