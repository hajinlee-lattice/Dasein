package com.latticeengines.domain.exposed.datacloud.match;

public class DnBMatchOutput {
    private String duns;

    private String confidenceCode;

    private String matchGrade;

    private DnBReturnCode dnbCode;

    public String getDuns() {
        return duns;
    }

    public void setDuns(String duns) {
        this.duns = duns;
    }

    public String getConfidenceCode() {
        return confidenceCode;
    }

    public void setConfidenceCode(String confidenceCode) {
        this.confidenceCode = confidenceCode;
    }

    public String getMatchGrade() {
        return matchGrade;
    }

    public void setMatchGrade(String matchGrade) {
        this.matchGrade = matchGrade;
    }

    public DnBReturnCode getDnbCode() {
        return dnbCode;
    }

    public void setDnbCode(DnBReturnCode dnbCode) {
        this.dnbCode = dnbCode;
    }
    
    
}
