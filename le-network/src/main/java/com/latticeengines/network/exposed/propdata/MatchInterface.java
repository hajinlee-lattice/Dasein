package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.propdata.manage.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.MatchOutput;

public interface MatchInterface {
    MatchOutput match(MatchInput input, Boolean returnUnmatched);
}
