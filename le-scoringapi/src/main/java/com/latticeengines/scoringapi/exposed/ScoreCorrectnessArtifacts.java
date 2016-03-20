package com.latticeengines.scoringapi.exposed;

import com.latticeengines.domain.exposed.scoringapi.DataComposition;

public class ScoreCorrectnessArtifacts {

    private String idField;
    private DataComposition dataScienceDataComposition;
    private String expectedRecords;
    private String scoredTxt;

    public void setIdField(String idFieldName) {
        this.idField = idFieldName;
    }

    public String getIdField() {
        return idField;
    }
    public DataComposition getDataScienceDataComposition() {
        return dataScienceDataComposition;
    }
    public void setDataScienceDataComposition(DataComposition dataScienceDataComposition) {
        this.dataScienceDataComposition = dataScienceDataComposition;
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

}
