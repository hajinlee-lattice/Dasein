package com.latticeengines.pls.service;

import java.io.InputStream;
import java.util.List;

import com.latticeengines.domain.exposed.workflow.Job;

public interface ScoringJobService {

    List<Job> getJobs(String modelId);

    InputStream getResults(String workflowJobId);

    String scoreTrainingData(String modelId);

}
