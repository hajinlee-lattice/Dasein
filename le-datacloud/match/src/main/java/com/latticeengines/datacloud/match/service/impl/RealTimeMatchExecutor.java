package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.metric.BulkMatchResponse;
import com.latticeengines.datacloud.match.metric.MatchResponse;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchFetcher;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.datacloud.match.MatchInput;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

@Component("realTimeMatchExecutor")
class RealTimeMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    private static final Log log = LogFactory.getLog(RealTimeMatchExecutor.class);

    @Autowired
    @Qualifier("realTimeMatchFetcher")
    private MatchFetcher fetcher;

    @Override
    @MatchStep
    public MatchContext execute(MatchContext matchContext) {
        matchContext = fetcher.fetch(matchContext);
        matchContext = complete(matchContext);
        generateOutputMetric(matchContext);
        return matchContext;
    }

    @Override
    @MatchStep(threshold = 10000L)
    public List<MatchContext> executeBulk(List<MatchContext> matchContexts) {
        if (matchContexts.isEmpty()) {
            return Collections.emptyList();
        }

        log.info("Enter executeBulk for " + matchContexts.size() + " match contexts.");

        MatchInput input = matchContexts.get(0).getInput();
        if (input == null) {
            throw new NullPointerException("Cannot find a MatchInput in MatchContext");
        }

        List<String> rootUids = fetcher.enqueue(matchContexts);
        List<MatchContext> outputContexts = fetcher.waitForResult(rootUids);
        for (MatchContext matchContext : outputContexts) {
            complete(matchContext);
        }
        return outputContexts;
    }

    @MatchStep
    private void generateOutputMetric(final MatchContext matchContext) {
        try {
            MatchResponse response = new MatchResponse(matchContext);
            metricService.write(MetricDB.LDC_Match, response);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.", e);
        }
    }

    @MatchStep
    public void generateOutputMetric(final BulkMatchOutput bulkMatchOutput) {
        try {
            BulkMatchResponse response = new BulkMatchResponse(bulkMatchOutput);
            metricService.write(MetricDB.LDC_Match, response);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.", e);
        }
    }
}
