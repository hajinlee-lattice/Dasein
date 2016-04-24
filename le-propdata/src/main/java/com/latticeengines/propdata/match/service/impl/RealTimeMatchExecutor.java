package com.latticeengines.propdata.match.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

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
    @Qualifier(value = "realTimeMatchFetcher")
    private MatchFetcher fetcher;

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

    @MatchStep
    private void generateOutputMetric(MatchContext matchContext) {
        try {
            MatchResponse response = new MatchResponse(matchContext);
            metricService.write(MetricDB.LDC_Match, response);
            generateAccountMetric(matchContext);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.", e);
        }
    }

}
