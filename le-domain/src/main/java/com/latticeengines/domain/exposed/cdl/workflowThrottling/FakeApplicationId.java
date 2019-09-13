package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class FakeApplicationId extends ApplicationId {
    public static final String fakedAppIdPrefix = "application_enqueued_";
    public static final int workflowJobPidIndex = 21;
    private String appId;

    public FakeApplicationId(String workflowJobPid) {
        appId = fakedAppIdPrefix + workflowJobPid;
    }

    public FakeApplicationId(Long workflowJobPid) {
        appId = fakedAppIdPrefix + workflowJobPid;
    }

    @Override
    public int getId() {
        return 0;
    }

    @Override
    protected void setId(int i) {
    }

    @Override
    public long getClusterTimestamp() {
        return 0;
    }

    @Override
    protected void setClusterTimestamp(long l) {
    }

    @Override
    protected void build() {
    }

    public static ApplicationId fromWorkflowJobPid(Long workflowJobPid) {
        return new FakeApplicationId(workflowJobPid);
    }

    public Long toWorkflowJobPid() {
        return Long.parseLong(appId.substring(workflowJobPidIndex));
    }

    @Override
    public String toString() {
        return appId;
    }
}
