package com.latticeengines.propdata.match.service;

import java.util.List;

import com.latticeengines.propdata.match.service.impl.MatchContext;

public interface MatchFetcher {

    MatchContext fetch(MatchContext matchContext);

    List<String> enqueue(List<MatchContext> matchContexts);

    List<MatchContext> waitForResult(List<String> rootUids);

}
