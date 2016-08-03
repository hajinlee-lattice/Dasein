package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.domain.exposed.workflow.Job;

public interface ScoringJobService {

    List<Job> getJobs(String modelId);

    InputStream getResults(String workflowJobId);

    String scoreTestingData(String modelId, String fileName, Boolean useRtsApi, Boolean performEnrichment);

    String scoreTrainingData(String modelId, Boolean useRtsApi, Boolean performEnrichmen);

    String getResultFileName(String workflowJobId);

    InputStream getScoringErrorStream(String jobId);

}
