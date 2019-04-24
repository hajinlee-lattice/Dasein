package com.latticeengines.objectapi.service.sparksql.impl;

import javax.inject.Inject;
import javax.inject.Named;

import org.springframework.stereotype.Service;

import com.latticeengines.domain.exposed.spark.LivySession;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.objectapi.service.impl.EntityQueryServiceImpl;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

@Service("entityQueryServiceSparkSQL")
public class EntityQueryServiceSparkSQLImpl extends EntityQueryServiceImpl {

    @Inject
    public EntityQueryServiceSparkSQLImpl(@Named("queryEvaluatorServiceSparkSQL") QueryEvaluatorService queryEvaluatorService,
            TransactionService transactionService) {
        super(queryEvaluatorService, transactionService);
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
