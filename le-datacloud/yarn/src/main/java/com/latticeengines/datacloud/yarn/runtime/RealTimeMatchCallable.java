package com.latticeengines.datacloud.yarn.runtime;

import java.util.concurrent.Callable;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.proxy.exposed.matchapi.MatchProxy;

public class RealTimeMatchCallable implements Callable<MatchContext> {

    private MatchInput matchInput;
    private String podId;
    private MatchPlanner matchPlanner;
    private MatchProxy matchProxy;

    RealTimeMatchCallable(MatchInput matchInput, String podId, MatchProxy matchProxy) {
        this.matchInput = matchInput;
        this.podId = podId;
        this.matchProxy = matchProxy;
    }

    @Override
    public MatchContext call() {
        HdfsPodContext.changeHdfsPodId(podId);
        return matchBlock(matchInput);
    }

    @MatchStep
    private MatchContext matchBlock(MatchInput input) {
        MatchContext matchContext = new MatchContext();
        matchContext.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        matchContext.setInput(input);
        try {
            MatchOutput output = matchProxy.matchRealTime(input);
            matchContext.setOutput(output);
            return matchContext;
        } catch (Exception e) {
            matchContext = matchPlanner.plan(input);
            return BulkMatchCallable.generateFakeOutput(matchContext, e);
        }
    }

}
