package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.workflow.Job;

public interface JobService {

    AppSubmission restart(String jobId);

    void cancel(String jobId);

    List<Job> findAllWithType(String type);

    Job findByApplicationId(String applicationId);

    Job find(String jobId);

    List<Job> findAll();

}
