package com.latticeengines.datacloud.workflow.match.steps;

public final class BulkMatchContextKey {

    protected BulkMatchContextKey() {
        throw new UnsupportedOperationException();
    }
    public static final String YARN_JOB_CONFIGS = "yarnJobConfigs";
    public static final String ROOT_OPERATION_UID = "rootOperationUid";
    public static final String APPLICATION_IDS = "applicationIds";
    public static final String MAX_CONCURRENT_BLOCKS = "maxConcurrentBlocks";
}
