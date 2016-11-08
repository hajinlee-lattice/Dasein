package com.latticeengines.datacloud.match.dnb;

public class DnBMatchGrade {

    private final String rawCode;

    public DnBMatchGrade(String rawCode) {
        this.rawCode = rawCode;
    }

    @Override
    public String toString() {
        return rawCode;
    }
}
