package com.latticeengines.datacloud.match.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.DnBMatchResultValidator;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;
import com.latticeengines.domain.exposed.datacloud.dnb.DnBReturnCode;

@Component("dnbMatchResultValidatorImpl")
public class DnBMatchResultValidatorImpl implements DnBMatchResultValidator {
    public boolean validate(DnBMatchOutput res) {
        if (res.getConfidenceCode() < 7) {
            res.setDuns(null);
            res.setDnbCode(DnBReturnCode.DISCARD);
            return false;
        }
        return true;
    }
}
