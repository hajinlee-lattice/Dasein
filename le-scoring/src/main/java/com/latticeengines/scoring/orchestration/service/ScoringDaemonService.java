package com.latticeengines.scoring.orchestration.service;

public interface ScoringDaemonService {

    int SUCCESS = 0;
    int FAIL = -1;

    String MODEL_GUID = "Model_GUID";
    String LEAD_SERIALIZE_TYPE_KEY = "SerializedValueAndType";
    String INPUT_COLUMN_METADATA = "InputColumnMetadata";
    String MODEL = "Model";
    String MODEL_NAME = "Name";
    String MODEL_COMPRESSED_SUPPORT_Files = "CompressedSupportFiles";
    String MODEL_SCRIPT = "Script";
    String SCORING_SCRIPT_NAME = "scoringengine.py";

    String NORMALIZATION_BUCKETS = "NormalizationBuckets";
    String NORMALIZATION_PROBABILITY = "Probability";
    String NORMALIZATION_EXPECTEDREVENUE = "ExpectedRevenue";
    String NORMALIZATION_START = "Start";
    String NORMALIZATION_END = "End";
    String NORMALIZATION_CUMULATIVEPERCENTAGE = "CumulativePercentage";

    String CALIBRATION = "Calibration";
    String CALIBRATION_MAXIMUMSCORE = "MaximumScore";
    String CALIBRATION_MINIMUMSCORE = "MinimumScore";
    String CALIBRATION_PROBABILITY = "Probability";
    String AVERAGE_PROBABILITY = "AverageProbability";
    String BUCKETS = "Buckets";
    String BUCKETS_TYPE = "Type";
    String BUCKETS_MAXIMUMSCORE = "Maximum";
    String BUCKETS_MINIMUMSCORE = "Minimum";
    String BUCKETS_NAME = "Name";
    String PERCENTILE_BUCKETS = "PercentileBuckets";
    String PERCENTILE_BUCKETS_PERCENTILE = "Percentile";
    String PERCENTILE_BUCKETS_MINIMUMSCORE = "MinimumScore";
    String PERCENTILE_BUCKETS_MAXIMUMSCORE = "MaximumScore";
    String SCORING_OUTPUT_PREFIX = "scoringoutputfile-";
    String AVRO_FILE_SUFFIX = ".avro";

    String INPUT_COLUMN_METADATA_NAME = "Name";
    String INPUT_COLUMN_METADATA_PURPOSE = "Purpose";
    String INPUT_COLUMN_METADATA_VALUETYPE = "ValueType";

    String JSON_SUFFIX = ".json";

    String SCORING_JOB_TYPE = "scoringJob";
    String UNIQUE_KEY_COLUMN = "LeadID";

    String IMPORT_ERROR_FILE_NAME = "error.csv";

}
