package com.latticeengines.propdata.match.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchInput;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.propdata.match.MatchInput;
import com.latticeengines.domain.exposed.propdata.match.MatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.MatchPlanner;
import com.latticeengines.propdata.match.service.RealTimeMatchService;

@Component("realTimeMatchWithDerivedColumnCacheService")
public class RealTimeMatchWithDerivedColumnCacheServiceImpl implements RealTimeMatchService {

    private static final String DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING = "1.";

    @Autowired
    @Qualifier("realTimeMatchPlanner")
    protected MatchPlanner matchPlanner;

    @Autowired
    @Qualifier("realTimeMatchExecutor")
    private MatchExecutor matchExecutor;

    @Override
    public boolean accept(String version) {
        if (StringUtils.isEmpty(version)
                || version.trim().startsWith(DEFAULT_VERSION_FOR_DERIVED_COLUMN_CACHE_BASED_MATCHING)) {
            return true;
        }

        return false;
    }

    @Override
    @MatchStep(threshold = 0L)
    public MatchOutput match(MatchInput input) {
        MatchContext matchContext = prepareMatchContext(input, null);
        matchContext = executeMatch(matchContext);
        return matchContext.getOutput();
    }

    @Override
    @MatchStep(threshold = 0L)
    public BulkMatchOutput matchBulk(BulkMatchInput input) {
        List<MatchContext> matchContexts = doPreProcessing(input);
        matchContexts = matchExecutor.executeBulk(matchContexts);
        return doPostProcessing(input, matchContexts);
    }

    protected MatchContext prepare(MatchInput input, List<ColumnMetadata> metadatas, boolean skipExecutionPlanning) {
        return matchPlanner.plan(input, metadatas, skipExecutionPlanning);
    }

    private MatchContext executeMatch(MatchContext context) {
        return matchExecutor.execute(context);
    }

    protected MatchContext prepareMatchContext(MatchInput input, List<ColumnMetadata> metadatas) {
        return prepareMatchContext(input, metadatas, false);
    }

    protected MatchContext prepareMatchContext(MatchInput input, List<ColumnMetadata> metadatas,
            boolean skipExecutionPlanning) {
        input.setUuid(UUID.randomUUID());
        MatchContext matchContext = prepare(input, metadatas, skipExecutionPlanning);
        matchContext.setMatchEngine(MatchContext.MatchEngine.REAL_TIME);
        matchContext.setReturnUnmatched(input.getReturnUnmatched());
        return matchContext;
    }

    private List<MatchContext> doPreProcessing(BulkMatchInput input) {
        List<MatchContext> matchContexts = new ArrayList<>(input.getInputList().size());
        List<ColumnMetadata> metadatas = null;
        for (MatchInput matchInput : input.getInputList()) {
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
