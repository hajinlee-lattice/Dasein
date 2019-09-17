package com.latticeengines.domain.exposed.cdl.workflowThrottling;

import org.apache.hadoop.yarn.api.records.ApplicationId;

public class FakeApplicationId extends ApplicationId {
    private static final String FAKE_APP_ID_PREFIX = "application_enqueued_";
    private static final int WORKFLOW_PID_INDEX = FAKE_APP_ID_PREFIX.length();
    private String appId;

    public FakeApplicationId(String workflowJobPid) {
        appId = FAKE_APP_ID_PREFIX + workflowJobPid;
    }

    public FakeApplicationId(Long workflowJobPid) {
        appId = FAKE_APP_ID_PREFIX + workflowJobPid;
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

    @Override
    public String toString() {
        return appId;
    }

    public static Long toWorkflowJobPid(String appId) {
        return Long.parseLong(appId.substring(WORKFLOW_PID_INDEX));
    }

    public static boolean isFakeApplicationId(String appId) {
        return appId.startsWith(FAKE_APP_ID_PREFIX);
    }
}
