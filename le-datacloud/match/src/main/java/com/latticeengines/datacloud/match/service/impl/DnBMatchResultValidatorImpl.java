package com.latticeengines.datacloud.match.service.impl;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBMatchGrade;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;

@Component("dnbMatchResultValidatorImpl")
public class DnBMatchResultValidatorImpl implements DnBMatchResultValidator {

    @Value("${datacloud.dnb.confidencecode.threshold}")
    private int confidenceCodeThreshold;

    public boolean validate(DnBMatchContext res) {
        if (discardConfidenceCode(res.getConfidenceCode()) && discardMatchGrade(res.getMatchGrade())) {
            res.setDnbCode(DnBReturnCode.DISCARD);
            return false;
        }
        return true;
    }

    private boolean discardConfidenceCode(Integer confidenceCode) {
        return confidenceCode != null && confidenceCode < confidenceCodeThreshold;
    }

    // There are 2 kinds of DnBMatchGrade: length = 7 and length = 11
    // DnBMatchGrade with length = 7 does not involve zipcode
    private boolean discardMatchGrade(DnBMatchGrade matchGrade) {
        if (matchGrade == null) {
            return false;
        }
        if ((matchGrade.getNameCode() != null && "A".equals(matchGrade.getNameCode())) //
                && (matchGrade.getCityCode() != null
                        && ("A".equals(matchGrade.getCityCode()) || "Z".equals(matchGrade.getCityCode()))) //
                && (matchGrade.getStateCode() != null
                        && ("A".equals(matchGrade.getStateCode()) || "Z".equals(matchGrade.getStateCode()))) //
                && (matchGrade.getPhoneCode() != null
                        && ("A".equals(matchGrade.getPhoneCode()) || "Z".equals(matchGrade.getPhoneCode()))) //
                && (matchGrade.getZipCodeCode() == null || "A".equals(matchGrade.getZipCodeCode())
                        || "Z".equals(matchGrade.getZipCodeCode()))) {
            return false;
        }
        return true;
    }
}
