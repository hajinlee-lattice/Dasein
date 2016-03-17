package com.latticeengines.domain.exposed.propdata.match;

import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

public class Matched implements Fact {

    private Boolean matched = false;

    public Matched() {
    }

    public Matched(Boolean matched) {
        this.matched = matched;
    }

    @MetricField(name = "Matched", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean getMatched() {
        return matched;
    }

    @MetricField(name = "Unmatched", fieldType = MetricField.FieldType.BOOLEAN)
    public Boolean getUnmatched() {
        return !matched;
    }

    public void setMatched(Boolean matched) {
        this.matched = matched;
    }

}
