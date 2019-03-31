package com.latticeengines.datacloud.yarn.runtime;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.Callable;

import com.latticeengines.datacloud.core.util.HdfsPodContext;
import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.datacloud.match.service.impl.InternalOutputRecord;
import com.latticeengines.datacloud.match.service.impl.MatchContext;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.OutputRecord;

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
        try {
            return matchExecutor.execute(matchContext);
        } catch (Exception e) {
            return generateFakeOutput(matchContext, e);
        }
    }

    static MatchContext generateFakeOutput(MatchContext matchContext, Throwable e) {
        List<OutputRecord> outputRecords = new ArrayList<>();
        for (InternalOutputRecord internalRecord: matchContext.getInternalResults()) {
            OutputRecord outputRecord = new OutputRecord();
            outputRecord.setOutput(null);
            outputRecord.setInput(internalRecord.getInput());
            outputRecord.setMatched(false);
            outputRecord.addErrorMessages(e.getMessage());

            outputRecord.setPreMatchDomain(internalRecord.getParsedDomain());
            outputRecord.setPreMatchNameLocation(internalRecord.getParsedNameLocation());
            outputRecord.setPreMatchDuns(internalRecord.getParsedDuns());
            outputRecord.setPreMatchEmail(internalRecord.getParsedEmail());

            outputRecord.setMatchedDomain(internalRecord.getMatchedDomain());
            outputRecord.setMatchedNameLocation(internalRecord.getMatchedNameLocation());
            outputRecord.setMatchedDuns(internalRecord.getMatchedDuns());
            outputRecord.setDnbCacheIds(internalRecord.getDnbCacheIds());
            outputRecord.setMatchedEmail(internalRecord.getMatchedEmail());
            outputRecord.setMatchedLatticeAccountId(internalRecord.getLatticeAccountId());

            outputRecord.setRowNumber(internalRecord.getRowNumber());
            outputRecord.setErrorMessages(internalRecord.getErrorMessages());
            outputRecord.setMatchLogs(internalRecord.getMatchLogs());
            outputRecord.setDebugValues(internalRecord.getDebugValues());
            outputRecords.add(outputRecord);
        }
        matchContext.getOutput().setResult(outputRecords);
        matchContext.getOutput().getStatistics().setRowsMatched(0);
        matchContext.getOutput().getStatistics().setOrphanedNoMatchCount(0L);
        matchContext.getOutput().getStatistics().setOrphanedUnmatchedAccountIdCount(0L);
        matchContext.getOutput().getStatistics().setMatchedByMatchKeyCount(0L);
        matchContext.getOutput().getStatistics().setMatchedByAccountIdCount(0L);
        return matchContext;
    }

}
