package com.latticeengines.domain.exposed.pls;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public class RatingEngineModelingParameters extends ModelingParameters {

    @JsonProperty
    private FrontEndQuery trainFilterQuery;

    @JsonProperty
    private FrontEndQuery targetFilterQuery;

    @JsonProperty
    private String tableName;

    @JsonProperty
    private String trainFilterTableName;

    @JsonProperty
    private String targetFilterTableName;

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getTrainFilterTableName() {
        return trainFilterTableName;
    }

    public void setTrainFilterTableName(String trainFilterTableName) {
        this.trainFilterTableName = trainFilterTableName;
    }

    public String getTargetFilterTableName() {
        return targetFilterTableName;
    }

    public void setTargetFilterTableName(String targetFilterTableName) {
        this.targetFilterTableName = targetFilterTableName;
    }

    public FrontEndQuery getTrainFilterQuery() {
        return trainFilterQuery;
    }

    public void setTrainFilterQuery(FrontEndQuery trainFilterQuery) {
        this.trainFilterQuery = trainFilterQuery;
    }

    public FrontEndQuery getTargetFilterQuery() {
        return targetFilterQuery;
    }

    public void setTargetFilterQuery(FrontEndQuery targetFilterQuery) {
        this.targetFilterQuery = targetFilterQuery;
    }

}
