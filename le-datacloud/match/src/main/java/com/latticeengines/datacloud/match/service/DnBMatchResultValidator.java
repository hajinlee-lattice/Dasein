package com.latticeengines.datacloud.match.service;

import com.latticeengines.datacloud.match.dnb.DnBMatchOutput;

public interface DnBMatchResultValidator {
    boolean validate(DnBMatchOutput res);
}
