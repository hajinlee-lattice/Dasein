package com.latticeengines.datacloud.match.service.impl;

import java.util.Collections;
import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.match.annotation.MatchStep;
import com.latticeengines.datacloud.match.exposed.service.BeanDispatcher;
import com.latticeengines.datacloud.match.service.DbHelper;
import com.latticeengines.datacloud.match.service.MatchExecutor;
import com.latticeengines.datacloud.match.service.MatchMetricService;
import com.latticeengines.domain.exposed.datacloud.match.BulkMatchOutput;

@Component("realTimeMatchExecutor")
class RealTimeMatchExecutor extends MatchExecutorBase implements MatchExecutor {

    private static final Logger log = LoggerFactory.getLogger(RealTimeMatchExecutor.class);

    @Autowired
    private BeanDispatcher beanDispatcher;

    @Inject
    @Lazy
    private MatchMetricService matchMetricService;

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
            matchMetricService.recordMatchFinished(matchContext);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.");
        }
    }

    @MatchStep
    public void generateOutputMetric(final BulkMatchOutput bulkMatchOutput) {
        try {
            matchMetricService.recordBulkSize(bulkMatchOutput);
        } catch (Exception e) {
            log.warn("Failed to extract output metric.");
        }
    }
}
