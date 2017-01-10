package com.latticeengines.datacloud.match.dnb;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

public class DnBMatchGrade implements Fact {

    private final String rawCode;
    private static final int NAME_BIT = 0;
    private static final int CITY_BIT = 3;
    private static final int STATE_BIT = 4;

    private String nameCode;
    private String cityCode;
    private String stateCode;

    public DnBMatchGrade(String rawCode) {
        this.rawCode = rawCode;
    }

    @MetricField(name = "DnBMatchGrade")
    public String getRawCode() {
        return rawCode;
    }

    @MetricField(name = "NameCode")
    public String getNameCode() {
        return String.valueOf(rawCode.charAt(NAME_BIT));
    }

    @MetricField(name = "CityCode")
    public String getCityCode() {
        return String.valueOf(rawCode.charAt(CITY_BIT));
    }

    @MetricField(name = "StateCode")
    public String getStateCode() {
        return String.valueOf(rawCode.charAt(STATE_BIT));
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
