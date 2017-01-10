package com.latticeengines.datacloud.match.service.impl;

import org.apache.commons.lang.StringUtils;
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

    private boolean discardMatchGrade(DnBMatchGrade matchGrade) {
        if (matchGrade == null) {
            return false;
        }
        if (StringUtils.isEmpty(matchGrade.getRawCode()) || matchGrade.getRawCode().length() < 5) {
            return true;
        }
        if ("A".equals(matchGrade.getNameCode()) //
                && ("A".equals(matchGrade.getCityCode()) || "Z".equals(matchGrade.getCityCode())) //
                && ("A".equals(matchGrade.getStateCode()) || "Z".equals(matchGrade.getStateCode()))) {
            return false;
        }
        return true;
    }
}
