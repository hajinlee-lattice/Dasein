package com.latticeengines.datacloud.match.service.impl;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchFetcher;


@Component("bulkMatchExecutor")
class BulkMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    @Autowired
    @Qualifier(value = "bulkMatchFetcher")
    private MatchFetcher fetcher;

    @Override
    public MatchContext execute(MatchContext matchContext) {
        matchContext = fetcher.fetch(matchContext);
        matchContext = complete(matchContext);
        return matchContext;
    }

    @Override
    public List<MatchContext> executeBulk(List<MatchContext> matchContexts) {
        throw new UnsupportedOperationException("This method is not supported.");
    }

}
