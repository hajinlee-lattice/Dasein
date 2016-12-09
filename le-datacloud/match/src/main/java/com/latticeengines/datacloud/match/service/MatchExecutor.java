package com.latticeengines.datacloud.match.service;

import java.util.List;

import com.latticeengines.datacloud.match.service.impl.MatchContext;



public interface MatchExecutor {

    MatchContext execute(MatchContext matchContext);

    List<MatchContext> executeBulk(List<MatchContext> matchContexts);

    MatchContext executeAsync(MatchContext matchContext);

    MatchContext executeMatchResult(MatchContext matchContext);


}
