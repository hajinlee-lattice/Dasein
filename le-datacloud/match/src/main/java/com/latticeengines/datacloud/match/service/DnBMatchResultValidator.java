package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.dnb.DnBMatchContext;

public interface DnBMatchResultValidator {

    boolean validate(DnBMatchContext res);
}
