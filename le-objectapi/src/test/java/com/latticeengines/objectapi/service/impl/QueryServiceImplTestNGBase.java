package com.latticeengines.objectapi.service.impl;

import static org.mockito.ArgumentMatchers.any;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.functionalframework.ObjectApiFunctionalTestNGBase;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.proxy.exposed.metadata.DataCollectionProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;

public abstract class QueryServiceImplTestNGBase extends ObjectApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(QueryServiceImplTestNGBase.class);

    @Inject
    private TransactionService transactionService;

    @Inject
    private QueryEvaluatorService queryEvaluatorService;

    protected Tenant tenant;

    protected void setup() {
        mockDataCollectionProxy();
        mockPeriodProxy();
        tenant = new Tenant("LocalTest");
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);
        String maxTransactionDate = transactionService.getMaxTransactionDate(DataCollection.Version.Blue);
        log.info("Max txn date is " + maxTransactionDate);
    }

    private void mockDataCollectionProxy() {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any(), any())).thenReturn(attrRepo);
        queryEvaluatorService.setDataCollectionProxy(proxy);
    }

    private void mockPeriodProxy() {
        PeriodProxy periodProxy = Mockito.mock(PeriodProxy.class);
        Mockito.when(periodProxy.getPeriodStrategies(any())).thenReturn(PeriodStrategy.NATURAL_PERIODS);
        ((TransactionServiceImpl) transactionService).setPeriodProxy(periodProxy);
    }

}
