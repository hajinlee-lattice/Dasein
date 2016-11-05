package com.latticeengines.datacloud.match.service;

import com.latticeengines.domain.exposed.datacloud.dnb.DnBMatchOutput;

public interface DnBMatchResultValidator {
    boolean validate(DnBMatchOutput res);
}
