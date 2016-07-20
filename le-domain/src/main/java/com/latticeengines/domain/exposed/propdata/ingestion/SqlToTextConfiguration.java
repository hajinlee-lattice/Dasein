package com.latticeengines.domain.exposed.propdata.ingestion;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;

public class SqlToTextConfiguration extends SqlConfiguration{
    private SqoopImporter.Mode sqoopMode;
    private String dbQuery;
    private String splitColumn;
    private int mappers;
    private String fileNamePrefix;
    private String fileExtension;
    private boolean compressed;
    private String nullString;
    private String enclosedBy;
    private String optionalEnclosedBy;
    private String escapedBy;
    private String fieldTerminatedBy;

    @JsonProperty("DbQuery")
    public String getDbQuery() {
        return dbQuery;
    }

    @JsonProperty("DbQuery")
    public void setDbQuery(String dbQuery) {
        this.dbQuery = dbQuery;
    }

    @JsonProperty("SqoopMode")
    public SqoopImporter.Mode getSqoopMode() {
        return sqoopMode;
    }

    @JsonProperty("SqoopMode")
    public void setSqoopMode(SqoopImporter.Mode sqoopMode) {
        this.sqoopMode = sqoopMode;
    }

    @JsonProperty("SplitColumn")
    public String getSplitColumn() {
        return splitColumn;
    }

    @JsonProperty("SplitColumn")
    public void setSplitColumn(String splitColumn) {
        this.splitColumn = splitColumn;
    }

    @JsonProperty("Mappers")
    public int getMappers() {
        return mappers;
    }

    @JsonProperty("Mappers")
    public void setMappers(int mappers) {
        this.mappers = mappers;
    }

    @JsonProperty("FileNamePrefix")
    public String getFileNamePrefix() {
        return fileNamePrefix;
    }

    @JsonProperty("FileNamePrefix")
    public void setFileNamePrefix(String fileNamePrefix) {
        this.fileNamePrefix = fileNamePrefix;
    }

    @JsonProperty("FileExtension")
    public String getFileExtension() {
        return fileExtension;
    }

    @JsonProperty("FileExtension")
    public void setFileExtension(String fileExtension) {
        this.fileExtension = fileExtension;
    }

    @JsonProperty("Compressed")
    public boolean isCompressed() {
        return compressed;
    }

    @JsonProperty("Compressed")
    public void setCompressed(boolean compressed) {
        this.compressed = compressed;
    }

    @JsonProperty("NullString")
    public String getNullString() {
        return nullString;
    }

    @JsonProperty("NullString")
    public void setNullString(String nullString) {
        this.nullString = nullString;
    }

    @JsonProperty("EnclosedBy")
    public String getEnclosedBy() {
        return enclosedBy;
    }

    @JsonProperty("EnclosedBy")
    public void setEnclosedBy(String enclosedBy) {
        this.enclosedBy = enclosedBy;
    }

    @JsonProperty("OptionalEnclosedBy")
    public String getOptionalEnclosedBy() {
        return optionalEnclosedBy;
    }

    @JsonProperty("OptionalEnclosedBy")
    public void setOptionalEnclosedBy(String optionalEnclosedBy) {
        this.optionalEnclosedBy = optionalEnclosedBy;
    }

    @JsonProperty("EscapedBy")
    public String getEscapedBy() {
        return escapedBy;
    }

    @JsonProperty("EscapedBy")
    public void setEscapedBy(String escapedBy) {
        this.escapedBy = escapedBy;
    }

    @JsonProperty("FieldTerminatedBy")
    public String getFieldTerminatedBy() {
        return fieldTerminatedBy;
    }

    @JsonProperty("FieldTerminatedBy")
    public void setFieldTerminatedBy(String fieldTerminatedBy) {
        this.fieldTerminatedBy = fieldTerminatedBy;
    }

}
