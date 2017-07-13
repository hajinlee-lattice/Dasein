package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.BeanDispatcher;
import com.latticeengines.datacloud.match.metric.BulkMatchResponse;
import com.latticeengines.datacloud.match.metric.MatchResponse;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;
import com.latticeengines.domain.exposed.monitor.metric.MetricDB;

@Component("realTimeMatchExecutor")
class RealTimeMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    private static final Logger log = LoggerFactory.getLogger(RealTimeMatchExecutor.class);

    @Autowired
    private BeanDispatcher beanDispatcher;

    @Override
    @MatchStep
    public MatchContext execute(MatchContext matchContext) {
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContext);
        matchContext = dbHelper.fetch(matchContext);
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
        DbHelper dbHelper = beanDispatcher.getDbHelper(matchContexts.get(0));
        List<MatchContext> outputContexts = dbHelper.fetch(matchContexts);
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
            log.warn("Failed to extract output metric.");
        }
    }

    @MatchStep
    public void generateOutputMetric(final BulkMatchOutput bulkMatchOutput) {
        try {
            BulkMatchResponse response = new BulkMatchResponse(bulkMatchOutput);
            metricService.write(MetricDB.LDC_Match, response);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.");
        }
    }
}
