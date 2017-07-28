package com.latticeengines.aws.batch;

public interface BatchService {

    String submitJob(JobRequest request);

    boolean waitForCompletion(String jobId, long maxWaitTime);
}
