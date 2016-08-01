package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.propdata.match.service.impl.MatchContext;

public interface MatchExecutor {

    MatchContext execute(MatchContext matchContext);

    List<MatchContext> executeBulk(List<MatchContext> matchContexts);

}
