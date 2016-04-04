package com.latticeengines.propdata.match.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchExecutor;

@Component("bulkMatchExecutor")
class BulkMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    private static final Log log = LogFactory.getLog(BulkMatchExecutor.class);

    @Override
    @MatchStep
    public MatchContext execute(MatchContext matchContext) {
        matchContext = fetch(matchContext);
        matchContext = complete(matchContext);
        return matchContext;
    }

}
