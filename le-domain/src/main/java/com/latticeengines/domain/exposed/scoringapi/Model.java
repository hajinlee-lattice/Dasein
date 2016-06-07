package com.latticeengines.domain.exposed.scoringapi;

import io.swagger.annotations.ApiModelProperty;

import com.fasterxml.jackson.annotation.JsonProperty;

public class Model {

    public static final String HDFS_SCORE_ARTIFACT_EVENTTABLE_DIR = "/user/s-analytics/customers/%s/data/%s-Event-Metadata/";
    public static final String HDFS_SCORE_ARTIFACT_APPID_DIR = "/user/s-analytics/customers/%s/models/%s/%s/";
    public static final String HDFS_SCORE_ARTIFACT_BASE_DIR = HDFS_SCORE_ARTIFACT_APPID_DIR + "%s/";
    public static final String HDFS_ENHANCEMENTS_DIR = "enhancements/";
    public static final String PMML_FILENAME = "rfpmml.xml";
    public static final String SCORE_DERIVATION_FILENAME = "scorederivation.json";
    public static final String DATA_COMPOSITION_FILENAME = "datacomposition.json";
    public static final String MODEL_JSON = "model.json";
    public static final String DATA_EXPORT_CSV = "_dataexport.csv";
    public static final String SAMPLES_AVRO_PATH = "/user/s-analytics/customers/%s/data/%s/samples/";
    public static final String SCORED_TXT = "_scored.txt";

    @JsonProperty("modelId")
    @ApiModelProperty(required = true, value = "Unique model id")
    private String modelId;

    @JsonProperty("name")
    @ApiModelProperty(value = "User customizable model name")
    private String name;

    @JsonProperty("type")
    @ApiModelProperty(required = true, value = "Model Type", allowableValues = "account, contact")
    private ModelType type;

    public Model() {
    }

    public Model(String modelId, String name, ModelType type) {
        this.modelId = modelId;
        this.name = name;
        this.type = type;
    }

    public String getModelId() {
        return modelId;
    }

    public String getName() {
        return name;
    }

}
