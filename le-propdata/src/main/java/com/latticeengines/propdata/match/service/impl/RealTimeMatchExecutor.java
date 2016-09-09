package com.latticeengines.propdata.match.service.impl;

import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.match.BulkMatchOutput;
import com.latticeengines.propdata.match.annotation.MatchStep;
import com.latticeengines.propdata.match.metric.BulkMatchResponse;
import com.latticeengines.propdata.match.metric.MatchResponse;
import com.latticeengines.propdata.match.service.MatchExecutor;
import com.latticeengines.propdata.match.service.MatchFetcher;

@Component("realTimeMatchExecutor")
class RealTimeMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    private static final Log log = LogFactory.getLog(RealTimeMatchExecutor.class);

    @Autowired
    @Qualifier("realTimeMatchFetcher")
    private MatchFetcher fetcher;

    @Autowired
    @Qualifier("matchExecutor")
    private ThreadPoolTaskExecutor matchExecutor;

    @Override
    @MatchStep
    public MatchContext execute(MatchContext matchContext) {
        matchContext = fetcher.fetch(matchContext);
        matchContext = doPostProcessing(matchContext);
        generateOutputMetric(matchContext);
        return matchContext;
    }

    private MatchContext doPostProcessing(MatchContext matchContext) {
        matchContext = complete(matchContext);
        return matchContext;
    }

    @Override
    @MatchStep(threshold = 10000L)
    public List<MatchContext> executeBulk(List<MatchContext> matchContexts) {
        List<String> rootUids = fetcher.enqueue(matchContexts);
        matchContexts = fetcher.waitForResult(rootUids);
        for (MatchContext matchContext : matchContexts) {
            doPostProcessing(matchContext);
        }
        return matchContexts;
    }

    @MatchStep
    private void generateOutputMetric(final MatchContext matchContext) {
        matchExecutor.execute(new Runnable() {
            @Override
            public void run() {
                generateMetric(matchContext);
            }
        });
    }

    @MatchStep
    public void generateOutputMetric(final BulkMatchOutput bulkMatchOutput) {
        matchExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    BulkMatchResponse response = new BulkMatchResponse(bulkMatchOutput);
                    metricService.writeSync(MetricDB.LDC_Match, response);
                } catch (Exception e) {
                    log.warn("Failed to extract output metric.", e);
                }
            }
        });
    }

    private void generateMetric(final MatchContext matchContext) {
        try {
            MatchResponse response = new MatchResponse(matchContext);
            metricService.writeSync(MetricDB.LDC_Match, response);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.", e);
        }
    }
}
