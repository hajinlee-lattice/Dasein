package com.latticeengines.datacloud.match.dnb;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

public class DnBMatchGrade implements Fact {

    private final String rawCode;

    public DnBMatchGrade(String rawCode) {
        this.rawCode = rawCode;
    }

    @MetricField(name = "DnBMatchGrade")
    public String getRawCode() {
        return rawCode;
    }

    @Override
    public boolean equals(Object that) {
        if (that instanceof DnBMatchGrade) {
            DnBMatchGrade matchGrade = (DnBMatchGrade) that;
            return StringUtils.equals(this.rawCode, matchGrade.rawCode);
        } else {
            return false;
        }
    }
}
