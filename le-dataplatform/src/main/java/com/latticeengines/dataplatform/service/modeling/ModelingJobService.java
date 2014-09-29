package com.latticeengines.dataplatform.service.modeling;

import java.util.List;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import com.latticeengines.dataplatform.service.JobService;
import com.latticeengines.domain.exposed.dataplatform.DbCreds;
import com.latticeengines.domain.exposed.dataplatform.Job;
import com.latticeengines.domain.exposed.dataplatform.JobStatus;

public interface ModelingJobService extends JobService {

    ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols);

    ApplicationId loadData(String table, String targetDir, DbCreds creds, String queue, String customer,
            List<String> splitCols, int numMappers);

    ApplicationId resubmitPreemptedJob(Job job);

    JobStatus getJobStatus(String applicationId, String hdfsPath) throws Exception;

}
