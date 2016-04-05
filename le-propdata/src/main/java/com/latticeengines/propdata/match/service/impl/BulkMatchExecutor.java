package com.latticeengines.propdata.match.service.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchExecutor;

@Component("bulkMatchExecutor")
class BulkMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    @Override
    @MatchStep
    public MatchContext execute(MatchContext matchContext) {
        matchContext = fetch(matchContext);
        matchContext = complete(matchContext);
        return matchContext;
    }

}
