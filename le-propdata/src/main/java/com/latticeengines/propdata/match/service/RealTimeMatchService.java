package com.latticeengines.propdata.match.service;

import com.latticeengines.domain.exposed.propdata.manage.MatchInput;
import com.latticeengines.domain.exposed.propdata.manage.MatchOutput;

public interface RealTimeMatchService {
    MatchOutput match(MatchInput input, boolean returnUnmatched);
}
