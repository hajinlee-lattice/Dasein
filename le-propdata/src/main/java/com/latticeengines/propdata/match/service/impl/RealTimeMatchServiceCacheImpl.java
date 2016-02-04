package com.latticeengines.propdata.match.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchStatus;
import com.latticeengines.propdata.core.service.ZkConfigurationService;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.propdata.match.service.RealTimeMatchService;

@Component("realTimeMatchServiceCache")
public class RealTimeMatchServiceCacheImpl implements RealTimeMatchService {

    private static Log log = LogFactory.getLog(RealTimeMatchServiceCacheImpl.class);

    @Autowired
    private MatchPlanner matchPlanner;

    @Autowired
    private RealTimeMatchExecutor matchExecutor;

    @Autowired
    private ZkConfigurationService zkConfigurationService;

    public MatchOutput match(MatchInput input, boolean returnUnmatched) {
        Long startTime = System.currentTimeMillis();

        validateMatchInput(input);
        MatchContext matchContext = prepare(input);
        matchContext = executeMatch(matchContext);

        matchContext.setStatus(MatchStatus.FINISHED);
        LoggingUtils.logInfoWithDuration(log, "Finished whole match for " + input.getData().size() + " rows.",
                matchContext, startTime);
        return matchContext.getOutput();
    }

    private void validateMatchInput(MatchInput input) {
        Long startTime = System.currentTimeMillis();

        MatchInputValidator.validate(input, zkConfigurationService.maxRealTimeInput());

        log.info("Finished validating match input for " + input.getData().size() + " rows. Duration="
                + (System.currentTimeMillis() - startTime));
    }

    private MatchContext prepare(MatchInput input) {
        Long startTime = System.currentTimeMillis();

        MatchContext context = matchPlanner.plan(input);

        LoggingUtils.logInfoWithDuration(log,
                "Finished preparing match context for " + input.getData().size() + " rows.", context, startTime);
        return context;
    }

    private MatchContext executeMatch(MatchContext context) {
        Long startTime = System.currentTimeMillis();

        context = matchExecutor.executeMatch(context);

        LoggingUtils.logInfoWithDuration(log,
                "Finished fetching data for " + context.getOutput().getStatistics().getRowsRequested() + " rows.",
                context, startTime);
        return context;
    }
}
