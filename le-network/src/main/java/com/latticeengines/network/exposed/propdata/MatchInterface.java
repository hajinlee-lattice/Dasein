package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;

public interface MatchInterface {
    MatchOutput match(MatchInput input, Boolean returnUnmatched);
}
