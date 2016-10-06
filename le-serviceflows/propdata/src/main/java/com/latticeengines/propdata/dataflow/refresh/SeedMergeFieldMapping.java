package com.latticeengines.propdata.dataflow.refresh;

import com.fasterxml.jackson.annotation.JsonProperty;

public class SeedMergeFieldMapping {
    private String targetColumn;

    private String dnbColumn;

    private String leColumn;

    private Boolean isDuns;

    private Boolean isDomain;

    @JsonProperty("TargetColumn")
    public String getTargetColumn() {
        return targetColumn;
    }

    @JsonProperty("TargetColumn")
    public void setTargetColumn(String targetColumn) {
        this.targetColumn = targetColumn;
    }

    @JsonProperty("DnBColumn")
    public String getDnbColumn() {
        return dnbColumn;
    }

    @JsonProperty("DnBColumn")
    public void setDnbColumn(String dnbColumn) {
        this.dnbColumn = dnbColumn;
    }

    @JsonProperty("LeColumn")
    public String getLeColumn() {
        return leColumn;
    }

    @JsonProperty("LeColumn")
    public void setLeColumn(String leColumn) {
        this.leColumn = leColumn;
    }

    @JsonProperty("IsDuns")
    public Boolean getIsDuns() {
        return isDuns;
    }

    @JsonProperty("IsDuns")
    public void setIsDuns(Boolean isDuns) {
        this.isDuns = isDuns;
    }

    @JsonProperty("IsDomain")
    public Boolean getIsDomain() {
        return isDomain;
    }

    @JsonProperty("IsDomain")
    public void setIsDomain(Boolean isDomain) {
        this.isDomain = isDomain;
    }
}
