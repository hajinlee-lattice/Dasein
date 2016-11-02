package com.latticeengines.datacloud.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.RealTimeMatchService;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchPlanner;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchInput;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.datacloud.match.MatchOutput;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;

@Component("realTimeMatchService")
public class RealTimeMatchServiceImpl implements RealTimeMatchService {

    @Autowired
    @Qualifier("realTimeMatchPlanner")
    protected MatchPlanner matchPlanner;

    @Autowired
    @Qualifier("realTimeMatchExecutor")
    private MatchExecutor matchExecutor;

    @MatchStep(threshold = 0L)
    public MatchOutput match(MatchInput input) {
        MatchContext matchContext = prepareMatchContext(input, null);
        matchContext = matchExecutor.execute(matchContext);
        return matchContext.getOutput();
    }

    @MatchStep(threshold = 0L)
    public BulkMatchOutput matchBulk(BulkMatchInput input) {
        long startTime = System.currentTimeMillis();
        List<MatchContext> matchContexts = doPreProcessing(input);
        matchContexts = matchExecutor.executeBulk(matchContexts);
        BulkMatchOutput bulkMatchOutput = doPostProcessing(input, matchContexts);
        bulkMatchOutput.setTimeElapsed(System.currentTimeMillis() - startTime);
        bulkMatchOutput.setMatchEngine(MatchContext.MatchEngine.REAL_TIME.getName());
        ((RealTimeMatchExecutor) matchExecutor).generateOutputMetric(bulkMatchOutput);
        return bulkMatchOutput;
    }

    private MatchContext prepareMatchContext(MatchInput input, List<ColumnMetadata> metadatas) {
        return prepareMatchContext(input, metadatas, false);
    }

    protected MatchContext prepareMatchContext(MatchInput input, List<ColumnMetadata> metadatas,
                                               boolean skipExecutionPlanning) {
        if (StringUtils.isEmpty(input.getRootOperationUid())) {
            input.setRootOperationUid(UUID.randomUUID().toString());
        }
        MatchContext matchContext = matchPlanner.plan(input, metadatas, skipExecutionPlanning);
        matchContext.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        matchContext.setReturnUnmatched(input.getReturnUnmatched());
        return matchContext;
    }

    private List<MatchContext> doPreProcessing(BulkMatchInput input) {
        List<MatchContext> matchContexts = new ArrayList<>(input.getInputList().size());
        List<ColumnMetadata> metadatas = null;
        for (MatchInput matchInput : input.getInputList()) {
            matchInput.setRootOperationUid(input.getRequestId());
            MatchContext matchContext = prepareMatchContext(matchInput, metadatas);
            if (input.isHomogeneous() && metadatas == null) {
                metadatas = matchContext.getOutput().getMetadata();
            }
            matchContexts.add(matchContext);
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
