package com.latticeengines.scoringapi.exposed;

import java.util.Map;

import com.latticeengines.domain.exposed.scoringapi.FieldSchema;

public class ScoreCorrectnessArtifacts {

    private String idField;
    private String expectedRecords;
    private String scoredTxt;
    private String pathToSamplesAvro;
    private Map<String, FieldSchema> fieldSchemas;

    public String getPathToSamplesAvro() {
        return pathToSamplesAvro;
    }

    public void setPathToSamplesAvro(String pathToSamplesAvro) {
        this.pathToSamplesAvro = pathToSamplesAvro;
    }

    public void setIdField(String idFieldName) {
        this.idField = idFieldName;
    }

    public String getIdField() {
        return idField;
    }
    public String getExpectedRecords() {
        return expectedRecords;
    }
    public void setExpectedRecords(String expectedRecords) {
        this.expectedRecords = expectedRecords;
    }
    public String getScoredTxt() {
        return scoredTxt;
    }
    public void setScoredTxt(String scoredTxt) {
        this.scoredTxt = scoredTxt;
    }

    public Map<String, FieldSchema> getFieldSchemas() {
        return fieldSchemas;
    }

    public void setFieldSchemas(Map<String, FieldSchema> fieldSchemas) {
        this.fieldSchemas = fieldSchemas;
    }

}
