package com.latticeengines.domain.exposed.datacloud.dnb;

import org.apache.commons.lang3.StringUtils;

import com.latticeengines.common.exposed.metric.Fact;
import com.latticeengines.common.exposed.metric.annotation.MetricField;

// There are 2 kinds of DnBMatchGrade: len = 7 or len = 11
// DnBMatchGrade with length = 7 does not contain zipcode
public class DnBMatchGrade implements Fact {

    private final String rawCode;
    private static final int NAME_BIT = 0;
    private static final int CITY_BIT = 3;
    private static final int STATE_BIT = 4;
    private static final int PHONE_BIT = 6;
    private static final int ZIPCODE_BIT = 7;

    public DnBMatchGrade(String rawCode) {
        this.rawCode = rawCode;
    }

    @MetricField(name = "DnBMatchGrade")
    public String getRawCode() {
        return rawCode;
    }

    @MetricField(name = "NameCode")
    public String getNameCode() {
        if (StringUtils.isEmpty(rawCode) || rawCode.length() < NAME_BIT + 1) {
            return null;
        }
        return String.valueOf(rawCode.charAt(NAME_BIT));
    }

    @MetricField(name = "CityCode")
    public String getCityCode() {
        if (StringUtils.isEmpty(rawCode) || rawCode.length() < CITY_BIT + 1) {
            return null;
        }
        return String.valueOf(rawCode.charAt(CITY_BIT));
    }

    @MetricField(name = "StateCode")
    public String getStateCode() {
        if (StringUtils.isEmpty(rawCode) || rawCode.length() < STATE_BIT + 1) {
            return null;
        }
        return String.valueOf(rawCode.charAt(STATE_BIT));
    }

    @MetricField(name = "PhoneCode")
    public String getPhoneCode() {
        if (StringUtils.isEmpty(rawCode) || rawCode.length() < PHONE_BIT + 1) {
            return null;
        }
        return String.valueOf(rawCode.charAt(PHONE_BIT));
    }

    @MetricField(name = "ZipCodeCode")
    public String getZipCodeCode() {
        if (StringUtils.isEmpty(rawCode) || rawCode.length() < ZIPCODE_BIT + 1) {
            return null;
        }
        return String.valueOf(rawCode.charAt(ZIPCODE_BIT));
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
