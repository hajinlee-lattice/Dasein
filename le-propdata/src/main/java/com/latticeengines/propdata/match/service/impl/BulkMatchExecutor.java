package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.List;

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
        return matchContext;
    }

    @Override
    public List<MatchContext> execute(List<MatchContext> matchContexts) {
        List<MatchContext> result = new ArrayList<>(matchContexts.size());
        for (MatchContext matchContext : matchContexts) {
            result.add(execute(matchContext));
        }
        return result;
    }

}
