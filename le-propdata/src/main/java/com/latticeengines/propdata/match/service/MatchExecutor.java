package com.latticeengines.propdata.match.service;

import com.latticeengines.propdata.match.service.impl.MatchContext;

public interface MatchExecutor {

    MatchContext executeMatch(MatchContext matchContext);

}
