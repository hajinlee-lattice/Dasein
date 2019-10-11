package com.latticeengines.domain.exposed.serviceflows.scoring.spark;

import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.pls.BucketMetadata;
import com.latticeengines.domain.exposed.scoring.ScoreResultField;
import com.latticeengines.domain.exposed.spark.SparkJobConfig;

public class CombineInputTableWithScoreJobConfig extends SparkJobConfig {

    private static final long serialVersionUID = 8800002914742516363L;

    public static final String NAME = "combineInputTableWithScoreFlow";

    @JsonProperty("bucket_metadata")
    public List<BucketMetadata> bucketMetadata;

    @JsonProperty("model_type")
    public String modelType;

    @JsonProperty("bucket_metadata_map")
    public Map<String, List<BucketMetadata>> bucketMetadataMap;

    @JsonProperty("score_field_name")
    public String scoreFieldName = ScoreResultField.Percentile.displayName;

    @JsonProperty("id_column")
    public String idColumn = InterfaceName.InternalId.name();

    @JsonProperty("model_id_field")
    public String modelIdField;

    @Override
    @JsonProperty("Name")
    public String getName() {
        return NAME;
    }

}
