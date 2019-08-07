package com.latticeengines.domain.exposed.workflow;

public class WorkflowContextConstants {
    public static final String REPORTS = "__REPORTS__";
    public static final String OUTPUTS = "__OUTPUTS__";

    public static class Outputs {
        public static final String EXPORT_OUTPUT_PATH = "EXPORT_OUTPUT_PATH";
        public static final String ERROR_OUTPUT_PATH = "ERROR_OUTPUT_PATH";
        public static final String YARN_LOG_LINK_PATH = "YARN_LOG_LINK_PATH";
        public static final String POST_MATCH_EVENT_TABLE_EXPORT_PATH = "POST_MATCH_EVENT_TABLE_EXPORT_PATH";
        public static final String POST_MATCH_ERROR_EXPORT_PATH = "POST_MATCH_ERROR_EXPORT_PATH";
        public static final String PIVOT_SCORE_EVENT_EXPORT_PATH = "PIVOT_SCORE_EVENT_EXPORT_PATH";
        public static final String PIVOT_SCORE_AVRO_PATH = "PIVOT_SCORE_AVRO_PATH";
        public static final String ENRICHMENT_FOR_INTERNAL_ATTRIBUTES_ATTEMPTED = "ENRICHMENT_FOR_INTERNAL_ATTRIBUTES_ATTEMPTED";
        public static final String INTERNAL_ENRICHMENT_ATTRIBUTES_LIST = "INTERNAL_ENRICHMENT_ATTRIBUTES_LIST";
        public static final String DATAFEED_STATUS = "DATAFEED_STATUS";
        public static final String EAI_JOB_APPLICATION_ID = "EAI_JOB_APPLICATION_ID";
        public static final String EAI_JOB_INPUT_FILE_PATH = "EAI_JOB_INPUT_FILE_PATH";
        public static final String IMPACTED_BUSINESS_ENTITIES = "IMPACTED_BUSINESS_ENTITIES";
        public static final String DATAFEEDTASK_REGISTERED_TABLES = "DATAFEEDTASK_REGISTERED_TABLES";
        public static final String DATAFEEDTASK_IMPORT_ERROR_FILES = "DATAFEEDTASK_IMPORT_ERROR_FILES";
        public static final String IMPORT_WARNING = "IMPORT_WARNING";
    }

    public static class Inputs {
        public static final String MODEL_ID = "MODEL_ID";
        public static final String MODEL_DELETED = "MODEL_DELETED";
        public static final String MODEL_DISPLAY_NAME = "MODEL_DISPLAY_NAME";
        public static final String MODEL_TYPE = "MODEL_TYPE";
        public static final String SOURCE_FILE_EXISTS = "SOURCE_FILE_EXISTS";
        public static final String SOURCE_DISPLAY_NAME = "SOURCE_DISPLAY_NAME";
        public static final String SOURCE_FILE_NAME = "SOURCE_FILE_NAME";
        public static final String SOURCE_FILE_PATH = "SOURCE_FILE_PATH";
        public static final String JOB_TYPE = "JOB_TYPE";
        public static final String DATAFEED_STATUS = "DATAFEED_STATUS";
        public static final String INITIAL_DATAFEED_STATUS = "INITIAL_DATAFEED_STATUS";
        public static final String DATAFEEDTASK_IMPORT_IDENTIFIER = "DATAFEEDTASK_IMPORT_IDENTIFIER";
        public static final String S3_IMPORT_EMAIL_FLAG = "S3_IMPORT_EMAIL_FLAG";
        public static final String S3_IMPORT_EMAIL_INFO = "S3_IMPORT_EMAIL_INFO";
        public static final String CHILDREN_WORKFLOW_JOB_IDS = "CHILDREN_WORKFLOW_JOB_IDS";
        public static final String IMPORT_MIGRATE_TRACKING_ID = "IMPORT_MIGRATE_TRACKING_ID";
        public static final String ACTION_IDS = "ACTION_IDS";
        public static final String ACTION_ID = "ACTION_ID";
        public static final String DRAINING = "DRAINING";
        public static final String RATING_ENGINE_ID = "RATING_ENGINE_ID";
        public static final String RATING_MODEL_ID = "RATING_MODEL_ID";
        public static final String DATAFEED_EXECUTION_ID = "DATAFEED_EXECUTION_ID";
        public static final String ALWAYS_ON_CAMPAIGNS = "ALWAYS_ON_CAMPAIGNS";
        public static final String PLAY_NAME = "PLAY_NAME";
        public static final String PLAY_LAUNCH_ID = "PLAY_LAUNCH_ID";
    }
}
