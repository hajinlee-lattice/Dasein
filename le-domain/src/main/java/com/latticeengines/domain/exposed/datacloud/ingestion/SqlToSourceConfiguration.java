package com.latticeengines.domain.exposed.datacloud.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SqlToSourceConfiguration extends SqlConfiguration {
    private String source;
    private String timestampColumn;
    private CollectCriteria collectCriteria;
    private int mappers;

    @JsonProperty("Source")
    public String getSource() {
        return source;
    }

    @JsonProperty("Source")
    public void setSource(String source) {
        this.source = source;
    }

    @JsonProperty("TimestampColumn")
    public String getTimestampColumn() {
        return timestampColumn;
    }

    @JsonProperty("TimestampColumn")
    public void setTimestampColumn(String timestampColumn) {
        this.timestampColumn = timestampColumn;
    }

    @JsonProperty("CollectCriteria")
    public CollectCriteria getCollectCriteria() {
        return collectCriteria;
    }

    @JsonProperty("CollectCriteria")
    public void setCollectCriteria(CollectCriteria collectCriteria) {
        this.collectCriteria = collectCriteria;
    }

    @JsonProperty("Mappers")
    public int getMappers() {
        return mappers;
    }

    @JsonProperty("Mappers")
    public void setMappers(int mappers) {
        this.mappers = mappers;
    }

    public enum CollectCriteria {
        ALL_DATA, NEW_DATA
    }

}
