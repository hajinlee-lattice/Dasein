package com.latticeengines.datacloud.yarn.runtime;

import java.util.Random;
import java.util.concurrent.Callable;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;

public class BulkMatchCallable implements Callable<MatchContext> {

    private MatchInput matchInput;
    private String podId;
    private MatchPlanner matchPlanner;
    private MatchExecutor matchExecutor;

    BulkMatchCallable(MatchInput matchInput, String podId, MatchPlanner matchPlanner, MatchExecutor matchExecutor) {
        this.matchInput = matchInput;
        this.podId = podId;
        this.matchPlanner = matchPlanner;
        this.matchExecutor = matchExecutor;
    }

    @Override
    public MatchContext call() {
        HdfsPodContext.changeHdfsPodId(podId);
        try {
            Thread.sleep(new Random().nextInt(200));
        } catch (InterruptedException e) {
            // ignore
        }
        return matchBlock(matchInput);
    }

    @MatchStep
    private MatchContext matchBlock(MatchInput input) {
        MatchContext matchContext = matchPlanner.plan(input);
        return matchExecutor.execute(matchContext);
    }

}
