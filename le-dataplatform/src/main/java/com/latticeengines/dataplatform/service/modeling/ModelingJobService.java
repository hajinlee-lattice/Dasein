package com.latticeengines.dataplatform.service.modeling;

import java.util.List;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.dataplatform.exposed.service.JobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.modeling.ModelingJob;

public interface ModelingJobService extends JobService{

    ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue, String customer, List<String> splitCols);

    ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue, String customer, List<String> splitCols, int numMappers);

    ApplicationId resubmitPreemptedJob(ModelingJob modelingJob);

    ApplicationId submitJob(ModelingJob modelingJob);
}
