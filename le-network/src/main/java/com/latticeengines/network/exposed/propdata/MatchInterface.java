package com.latticeengines.network.exposed.propdata;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;

public interface MatchInterface {
    MatchOutput matchRealTime(MatchInput input, Boolean returnUnmatched);
}
