package com.latticeengines.datacloud.match.dnb;

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
}
