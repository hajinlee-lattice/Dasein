package com.latticeengines.datacloud.match.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.dnb.DnBMatchContext;
import com.latticeengines.datacloud.match.dnb.DnBReturnCode;
import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;

@Component("dnbMatchResultValidatorImpl")
public class DnBMatchResultValidatorImpl implements DnBMatchResultValidator {

    public boolean validate(DnBMatchContext res) {
        if (res.getConfidenceCode() == null || res.getConfidenceCode() < 7) {
            res.setDuns(null);
            res.setDnbCode(DnBReturnCode.DISCARD);
            return false;
        }
        return true;
    }
}
