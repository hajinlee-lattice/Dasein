package com.latticeengines.scoring.orchestration.service;

public interface ScoringDaemonService {

    public static final int SUCCESS = 0;
    public static final int FAIL = -1;

    public static final String MODEL_GUID = "Model_GUID";
    public static final String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    public static final String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    public static final String MODEL = "Model";
    public static final String MODEL_NAME = "Name";
    public static final String MODEL_COMPRESSED_SUPPORT_Files = "CompressedSupportFiles";
    public static final String MODEL_SCRIPT = "Script";
    public static final String SCORING_SCRIPT_NAME = "scoringengine.py";

    public static final String CALIBRATION = "Calibration";
    public static final String CALIBRATION_MAXIMUMSCORE = "MaximumScore";
    public static final String CALIBRATION_MINIMUMSCORE = "MinimumScore";
    public static final String CALIBRATION_PROBABILITY = "Probability";
    public static final String AVERAGE_PROBABILITY = "AverageProbability";
    public static final String BUCKETS = "Buckets";
    public static final String BUCKETS_TYPE = "Type";
    public static final String BUCKETS_MAXIMUMSCORE = "Maximum";
    public static final String BUCKETS_MINIMUMSCORE = "Minimum";
    public static final String BUCKETS_NAME = "Name";
    public static final String PERCENTILE_BUCKETS = "PercentileBuckets";
    public static final String PERCENTILE_BUCKETS_PERCENTILE = "Percentile";
    public static final String PERCENTILE_BUCKETS_MINIMUMSCORE = "MinimumScore";
    public static final String PERCENTILE_BUCKETS_MAXIMUMSCORE = "MaximumScore";
    public static final String SCORING_OUTPUT_PREFIX = "scoringoutputfile-";
    public static final String AVRO_FILE_SUFFIX = ".avro";

    public static final String INPUT_COLUMN_METADATA_NAME = "Name";
    public static final String INPUT_COLUMN_METADATA_PURPOSE = "Purpose";
    public static final String INPUT_COLUMN_METADATA_VALUETYPE = "ValueType";

    public static final String JSON_SUFFIX = ".json";
    
    public static final String COMMA = ",";
    
    public static final String SCORING_JOB_TYPE = "scoringJob";
    public static final String UNIQUE_KEY_COLUMN = "LeadID";
}
