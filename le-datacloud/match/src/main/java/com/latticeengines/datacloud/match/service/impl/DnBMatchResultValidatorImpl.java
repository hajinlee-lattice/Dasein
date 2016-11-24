package com.latticeengines.datacloud.match.service.impl;

import org.apache.commons.lang.StringUtils;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBMatchGrade;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;

@Component("dnbMatchResultValidatorImpl")
public class DnBMatchResultValidatorImpl implements DnBMatchResultValidator {

    public boolean validate(DnBMatchContext res) {
        if (discardConfidenceCode(res.getConfidenceCode()) && discardMatchGrade(res.getMatchGrade())) {
            res.setDuns(null);
            res.setDnbCode(DnBReturnCode.DISCARD);
            return false;
        }
        return true;
    }

    private boolean discardConfidenceCode(Integer confidenceCode) {
        return confidenceCode != null && confidenceCode < 7;
    }

    private boolean discardMatchGrade(DnBMatchGrade matchGrade) {
        if (matchGrade == null) {
            return false;
        }
        if (StringUtils.isEmpty(matchGrade.getRawCode()) || matchGrade.getRawCode().length() < 5) {
            return true;
        }
        // Name: 0, City: 3, State: 4
        if (matchGrade.getRawCode().charAt(0) == 'A' && matchGrade.getRawCode().charAt(3) == 'Z'
                && matchGrade.getRawCode().charAt(4) == 'A') {
            return false;
        }
        return true;
    }
}
