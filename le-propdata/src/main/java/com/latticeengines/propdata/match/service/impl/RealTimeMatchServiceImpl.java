package com.latticeengines.propdata.match.service.impl;

import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.MatchPlanner;
import com.latticeengines.propdata.match.service.RealTimeMatchService;

@Component("realTimeMatchService")
public class RealTimeMatchServiceImpl implements RealTimeMatchService {

    @Autowired
    @Qualifier("realTimeMatchPlanner")
    private MatchPlanner matchPlanner;

    @Autowired
    @Qualifier("realTimeMatchExecutor")
    private MatchExecutor matchExecutor;

    @Override
    @MatchStep
    public MatchOutput match(MatchInput input, boolean returnUnmatched) {
        input.setUuid(UUID.randomUUID());
        MatchContext matchContext = prepare(input);
        matchContext.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        matchContext.setReturnUnmatched(returnUnmatched);
        matchContext = executeMatch(matchContext);
        return matchContext.getOutput();
    }

    private MatchContext prepare(MatchInput input) {
        return matchPlanner.plan(input);
    }

    private MatchContext executeMatch(MatchContext context) {
        return matchExecutor.execute(context);
    }
}
