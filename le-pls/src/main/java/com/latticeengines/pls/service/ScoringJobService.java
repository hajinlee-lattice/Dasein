package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.workflow.Job;

public interface ScoringJobService {

    List<Job> getJobs(String modelId);

    InputStream getScoreResults(String workflowJobId);

    String scoreTestingData(String modelId, String fileName, Boolean performEnrichment, Boolean debug);

    String scoreTrainingData(String modelId, Boolean performEnrichmen, Boolean debug);

    String getResultScoreFileName(String workflowJobId);

    InputStream getScoringErrorStream(String jobId);

    InputStream getPivotScoringFile(String workflowJobId);

    String getResultPivotScoreFileName(String workflowJobId);

    String scoreRatinggData(String modelId, String displayName, EventFrontEndQuery targetQuery,
            String tableToScoreName);

}
