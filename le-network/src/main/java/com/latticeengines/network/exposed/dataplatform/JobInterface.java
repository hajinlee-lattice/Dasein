package com.latticeengines.network.exposed.dataplatform;

import org.apache.hadoop.mapreduce.v2.api.records.Counters;

import com.latticeengines.domain.exposed.dataplatform.JobStatus;

public interface JobInterface {

    JobStatus getJobStatus(String applicationId);

    Counters getMRJobCounters(String applicationId);
}
