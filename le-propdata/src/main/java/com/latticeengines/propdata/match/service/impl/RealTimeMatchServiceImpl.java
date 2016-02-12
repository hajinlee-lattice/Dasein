package com.latticeengines.propdata.match.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.RealTimeMatchService;

@Component("realTimeMatchService")
public class RealTimeMatchServiceImpl implements RealTimeMatchService {

    @Autowired
    private MatchPlanner matchPlanner;

    @Autowired
    @Qualifier(value = "realTimeMatchExecutor")
    private MatchExecutor matchExecutor;

    @Override
    @MatchStep
    public MatchOutput match(MatchInput input, boolean returnUnmatched) {
        MatchContext matchContext = prepare(input);
        matchContext.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        matchContext.setReturnUnmatched(returnUnmatched);
        matchContext = executeMatch(matchContext);

        matchContext.setStatus(MatchStatus.FINISHED);
        return matchContext.getOutput();
    }

    private MatchContext prepare(MatchInput input) {
        return matchPlanner.planForRealTime(input);
    }

    private MatchContext executeMatch(MatchContext context) {
        return matchExecutor.executeMatch(context);
    }
}
