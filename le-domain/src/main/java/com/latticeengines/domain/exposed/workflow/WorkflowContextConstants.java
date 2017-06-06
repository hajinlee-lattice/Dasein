package com.latticeengines.domain.exposed.workflow;

public class WorkflowContextConstants {
    public static final String REPORTS = "__REPORTS__";
    public static final String OUTPUTS = "__OUTPUTS__";

    public static class Outputs {
        public static final String EXPORT_OUTPUT_PATH = "EXPORT_OUTPUT_PATH";
        public static final String ERROR_OUTPUT_PATH = "ERROR_OUTPUT_PATH";
        public static final String YARN_LOG_LINK_PATH = "YARN_LOG_LINK_PATH";
        public static final String POST_MATCH_EVENT_TABLE_EXPORT_PATH = "POST_MATCH_EVENT_TABLE_EXPORT_PATH";
        public static final String PIVOT_SCORE_EVENT_EXPORT_PATH = "PIVOT_SCORE_EVENT_EXPORT_PATH";
        public static final String PIVOT_SCORE_AVRO_PATH = "PIVOT_SCORE_AVRO_PATH";
        public static final String ENRICHMENT_FOR_INTERNAL_ATTRIBUTES_ATTEMPTED = "ENRICHMENT_FOR_INTERNAL_ATTRIBUTES_ATTEMPTED";
        public static final String INTERNAL_ENRICHMENT_ATTRIBUTES_LIST = "INTERNAL_ENRICHMENT_ATTRIBUTES_LIST";
    }

    public static class Inputs {
        public static final String MODEL_ID = "MODEL_ID";
        public static final String MODEL_DELETED = "MODEL_DELETED";
        public static final String MODEL_DISPLAY_NAME = "MODEL_DISPLAY_NAME";
        public static final String MODEL_TYPE = "MODEL_TYPE";
        public static final String SOURCE_FILE_EXISTS = "SOURCE_FILE_EXISTS";
        public static final String SOURCE_DISPLAY_NAME = "SOURCE_DISPLAY_NAME";
        public static final String JOB_TYPE = "JOB_TYPE";
        public static final String DATAFEED_NAME = "DATAFEED_NAME";
    }
}
