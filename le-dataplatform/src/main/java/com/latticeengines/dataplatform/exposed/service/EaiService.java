package com.latticeengines.dataplatform.exposed.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;
import com.latticeengines.domain.exposed.eai.EaiConfiguration;

public interface EaiService {

    ApplicationId invokeEai(EaiConfiguration eaiConfig);

    JobStatus getJobStatus(String applicationId);
}
