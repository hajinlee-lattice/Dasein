package com.latticeengines.monitor.tracing;

/**
 * Tag constants for distributed tracing
 */
public class TracingTags {
    public static final String TENANT_ID = "tenant.id";
    public static final String ENTITY = "entity";

    public static class Workflow {
        public static final String PID = "workflow.pid";
        public static final String USER = "workflow.user";
        public static final String WORKFLOW_ID = "workflow.id";
        public static final String APPLICATION_ID = "workflow.app_id";
        public static final String WORKFLOW_NAME = "workflow.name";
        public static final String SOFTWARE_LIBRARIES = "workflow.swpkgs";
        public static final String IS_RESTART = "workflow.restart";
        public static final String NAMESPACE = "workflow.namespace";
        public static final String STEP = "workflow.step";
        public static final String STEP_SEQ = "workflow.step_seq";
    }

    public static class DataCloud {
        public static final String ROOT_OPERATION_UID = "datacloud.root_operation_uid";
        public static final String BLOCK_OPERATION_UID = "datacloud.block_operation_uid";
    }

    public static class Attribute {
        public static final String ATTR_CONFIG_UPDATE_MODE = "attr.config.update_mode";
    }
}
