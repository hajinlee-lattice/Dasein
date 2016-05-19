package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.match.BulkMatchInput;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchOutput;
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
    public MatchOutput match(MatchInput input) {
        MatchContext matchContext = prepareMatchContext(input);
        matchContext = executeMatch(matchContext);
        return matchContext.getOutput();
    }

    @Override
    @MatchStep
    public BulkMatchOutput match(BulkMatchInput input) {
        List<MatchContext> matchContexts = doPreProcessing(input);
        matchContexts = matchExecutor.execute(matchContexts);
        return doPostProcessing(input, matchContexts);
    }

    private MatchContext prepare(MatchInput input) {
        return matchPlanner.plan(input);
    }

    private MatchContext executeMatch(MatchContext context) {
        return matchExecutor.execute(context);
    }

    private MatchContext prepareMatchContext(MatchInput input) {
        input.setUuid(UUID.randomUUID());
        MatchContext matchContext = prepare(input);
        matchContext.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        matchContext.setReturnUnmatched(input.getReturnUnmatched());
        return matchContext;
    }

    private List<MatchContext> doPreProcessing(BulkMatchInput input) {
        List<MatchContext> matchContexts = new ArrayList<>(input.getInputList().size());
        for (MatchInput matchInput : input.getInputList()) {
            matchContexts.add(prepareMatchContext(matchInput));
        }
        return matchContexts;
    }

    private BulkMatchOutput doPostProcessing(BulkMatchInput input, List<MatchContext> matchContexts) {
        BulkMatchOutput bulkMatchOutput = new BulkMatchOutput();
        bulkMatchOutput.setRequestId(input.getRequestId());
        List<MatchOutput> outputList = new ArrayList<>(matchContexts.size());
        for (MatchContext matchContext : matchContexts) {
            outputList.add(matchContext.getOutput());
        }
        bulkMatchOutput.setOutputList(outputList);
        return bulkMatchOutput;
    }
}
