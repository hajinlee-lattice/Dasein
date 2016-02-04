package com.latticeengines.propdata.match.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.RealTimeMatchService;

@Component("realTimeMatchServiceCache")
public class RealTimeMatchServiceCacheImpl implements RealTimeMatchService {

    @Autowired
    private MatchPlanner matchPlanner;

    @Autowired
    @Qualifier(value = "realTimeMatchExecutor")
    private MatchExecutor matchExecutor;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    @Override
    @MatchStep
    public MatchOutput match(MatchInput input, boolean returnUnmatched) {
        validateMatchInput(input);
        MatchContext matchContext = prepare(input);
        matchContext = executeMatch(matchContext);

        matchContext.setStatus(MatchStatus.FINISHED);
        return matchContext.getOutput();
    }

    @MatchStep
    private void validateMatchInput(MatchInput input) {
        MatchInputValidator.validate(input, zkConfigurationService.maxRealTimeInput());
    }

    @MatchStep
    private MatchContext prepare(MatchInput input) {
        return matchPlanner.plan(input);
    }

    @MatchStep
    private MatchContext executeMatch(MatchContext context) {
        return matchExecutor.executeMatch(context);
    }
}
