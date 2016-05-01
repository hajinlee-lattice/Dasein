package com.latticeengines.propdata.match.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.MatchFetcher;

@Component("bulkMatchExecutor")
class BulkMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    @Autowired
    @Qualifier(value = "bulkMatchFetcher")
    private MatchFetcher fetcher;

    @Override
    @MatchStep
    public MatchContext execute(MatchContext matchContext) {
        matchContext = fetcher.fetch(matchContext);
        matchContext = complete(matchContext);
        generateAccountMetric(matchContext);
        return matchContext;
    }

}
