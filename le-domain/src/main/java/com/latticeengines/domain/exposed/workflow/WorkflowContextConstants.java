package com.latticeengines.domain.exposed.workflow;

public class WorkflowContextConstants {
    public static final String REPORTS = "__REPORTS__";
    public static final String OUTPUTS = "__OUTPUTS__";

    public static class Outputs {
        public static final String EXPORT_OUTPUT_PATH = "EXPORT_OUTPUT_PATH";
        public static final Object ERROR_OUTPUT_PATH = "ERROR_OUTPUT_PATH";
        public static final String MODEL_ID = "MODEL_ID";
    }

    public static class Inputs {
        public static final String MODEL_ID = "MODEL_ID";
        public static final String MODEL_NAME = "MODEL_NAME";
        public static final String SOURCE_DISPLAY_NAME = "SOURCE_DISPLAY_NAME";
        public static final String JOB_TYPE = "JOB_TYPE";
    }
}
