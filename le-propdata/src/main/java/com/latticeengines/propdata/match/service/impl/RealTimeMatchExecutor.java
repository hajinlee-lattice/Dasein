package com.latticeengines.propdata.match.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.propdata.match.annotation.MatchStep;
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
        matchContext = complete(matchContext);
        matchContext = appendMetadataToContext(matchContext);
        generateOutputMetric(matchContext);
        return matchContext;
    }

    public MatchContext appendMetadataToContext(MatchContext matchContext) {
        ColumnSelection.Predefined selection = matchContext.getInput().getPredefinedSelection();
        matchContext.setOutput(appendMetadata(matchContext.getOutput(), selection));
        return matchContext;
    }

    private void generateOutputMetric(final MatchContext matchContext) {
        matchExecutor.execute(new Runnable() {
            @Override
            public void run() {
                try {
                    MatchContext localContext = JsonUtils.deserialize(JsonUtils.serialize(matchContext), MatchContext.class);
                    MatchResponse response = new MatchResponse(localContext);
                    metricService.write(MetricDB.LDC_Match, response);
                    generateAccountMetric(localContext);
                } catch (Exception e) {
                    log.warn("Failed to extract output metric.", e);
                }
            }
        });
    }

}
