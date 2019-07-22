package com.latticeengines.objectapi.service.sparksql.impl;

import javax.inject.Inject;
import javax.inject.Named;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.objectapi.service.impl.EventQueryServiceImpl;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.factory.SparkQueryProvider;

@Service("eventQueryServiceSparkSQL")
public class EventQueryServiceSparkSQLImpl extends EventQueryServiceImpl {

    @Inject
    public EventQueryServiceSparkSQLImpl(@Named("queryEvaluatorServiceSparkSQL") QueryEvaluatorService queryEvaluatorService,
            TransactionService transactionService) {
        super(queryEvaluatorService, transactionService);
        setBatchUser(SparkQueryProvider.SPARK_BATCH_USER);
    }

    /**
     * @param livySession
     *
     * This is added for Testing Purpose. In real world, this session will be created at runtime
     */
    public void setLivySession(LivySession livySession) {
        ((QueryEvaluatorServiceSparkSQL)queryEvaluatorService).setLivySession(livySession);
    }

}
