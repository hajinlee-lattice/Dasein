package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchContext;

public interface DnBMatchResultValidator {

    boolean validate(DnBMatchContext res);
}
