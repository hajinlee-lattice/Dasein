package com.latticeengines.domain.exposed.pls;

import org.codehaus.jackson.annotate.JsonProperty;

import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public class RatingEngineModelingParameters extends ModelingParameters {

    @JsonProperty
    private EventFrontEndQuery trainFilterQuery;

    @JsonProperty
    private EventFrontEndQuery eventFilterQuery;

    @JsonProperty
    private EventFrontEndQuery targetFilterQuery;

    @JsonProperty
    private String tableName;

    @JsonProperty
    private String trainFilterTableName;

    @JsonProperty
    private String eventFilterTableName;

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

    public String getEventFilterTableName() {
        return eventFilterTableName;
    }

    public void setEventFilterTableName(String eventFilterTableName) {
        this.eventFilterTableName = eventFilterTableName;
    }

    public String getTargetFilterTableName() {
        return targetFilterTableName;
    }

    public void setTargetFilterTableName(String targetFilterTableName) {
        this.targetFilterTableName = targetFilterTableName;
    }

    public EventFrontEndQuery getTrainFilterQuery() {
        return trainFilterQuery;
    }

    public void setTrainFilterQuery(EventFrontEndQuery trainFilterQuery) {
        this.trainFilterQuery = trainFilterQuery;
    }

    public EventFrontEndQuery getEventFilterQuery() {
        return eventFilterQuery;
    }

    public void setEventFilterQuery(EventFrontEndQuery eventFilterQuery) {
        this.eventFilterQuery = eventFilterQuery;
    }

    public EventFrontEndQuery getTargetFilterQuery() {
        return targetFilterQuery;
    }

    public void setTargetFilterQuery(EventFrontEndQuery targetFilterQuery) {
        this.targetFilterQuery = targetFilterQuery;
    }

}
