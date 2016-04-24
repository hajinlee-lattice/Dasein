package com.latticeengines.propdata.match.service;

import com.latticeengines.propdata.match.service.impl.MatchContext;

public interface MatchFetcher {

    MatchContext fetch(MatchContext matchContext);

}
