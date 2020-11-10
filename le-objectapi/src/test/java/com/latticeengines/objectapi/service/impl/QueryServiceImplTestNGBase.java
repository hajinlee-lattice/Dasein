package com.latticeengines.objectapi.service.impl;

import static org.mockito.ArgumentMatchers.any;

import java.util.List;
import java.util.Map;

import javax.inject.Inject;

import org.mockito.Mockito;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.test.util.ReflectionTestUtils;
import org.testng.Assert;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.cdl.PeriodStrategy;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.InterfaceName;
import com.latticeengines.domain.exposed.query.AttributeLookup;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.objectapi.functionalframework.ObjectApiFunctionalTestNGBase;
import com.latticeengines.objectapi.service.TransactionService;
import com.latticeengines.proxy.exposed.cdl.DataCollectionProxy;
import com.latticeengines.proxy.exposed.cdl.PeriodProxy;
import com.latticeengines.query.exposed.evaluator.QueryEvaluatorService;
import com.latticeengines.query.factory.RedshiftQueryProvider;

public abstract class QueryServiceImplTestNGBase extends ObjectApiFunctionalTestNGBase {

    private static final Logger log = LoggerFactory.getLogger(QueryServiceImplTestNGBase.class);

    public static final String SEGMENT_USER = RedshiftQueryProvider.USER_SEGMENT;

    @Inject
    private TransactionService transactionService;

    @Inject
    QueryEvaluatorService queryEvaluatorService;

    protected Tenant tenant;
    protected String maxTransactionDate;

    protected void setupTestData(int dataVersion) {
        setupTestData(dataVersion, false, false);
    }

    protected void setupTestDataWithAthena(int dataVersion) {
        setupTestData(dataVersion, false, true);
    }

    protected void setupTestDataWithSpark(int dataVersion) {
        setupTestData(dataVersion, true, false);
    }

    private void setupTestData(int dataVersion, boolean setupSpark, boolean setupAthena) {
        initializeAttributeRepo(dataVersion, setupSpark, setupAthena);
        mockDataCollectionProxy(queryEvaluatorService);
        mockPeriodProxy();
        tenant = new Tenant();
        tenant.setId(attrRepo.getCustomerSpace().toString());
        tenant.setName(attrRepo.getCustomerSpace().getTenantId());
        tenant.setPid(1L);
        MultiTenantContext.setTenant(tenant);
        if (hasTransactionDate()) {
            maxTransactionDate = transactionService.getMaxTransactionDate(DataCollection.Version.Blue);
            log.info("Max txn date is " + maxTransactionDate);
        } else {
            log.info("No transaction in this attr repo.");
        }
    }

    private boolean hasTransactionDate() {
        AttributeLookup attr = new AttributeLookup(BusinessEntity.Transaction, InterfaceName.TransactionDate.name());
        return attrRepo.getColumnMetadata(attr) != null;
    }

    protected void mockDataCollectionProxy(QueryEvaluatorService queryEvaluatorService) {
        DataCollectionProxy proxy = Mockito.mock(DataCollectionProxy.class);
        Mockito.when(proxy.getAttrRepo(any(), any())).thenReturn(attrRepo);
        ReflectionTestUtils.setField(queryEvaluatorService, "dataCollectionProxy", proxy);
    }

    private void mockPeriodProxy() {
        PeriodProxy periodProxy = Mockito.mock(PeriodProxy.class);
        Mockito.when(periodProxy.getPeriodStrategies(any())).thenReturn(PeriodStrategy.NATURAL_PERIODS);
        ((TransactionServiceImpl) transactionService).setPeriodProxy(periodProxy);
    }

    protected long testAndAssertCount(String sqlUser, long resultCount, long expectedCount) {
        Assert.assertEquals(resultCount, expectedCount, "Counts Doesn't match");
        return resultCount;
    }

    protected List<Map<String, Object>> testAndAssertData(String sqlUser, List<Map<String, Object>> results,
            List<Map<String, Object>> expectedResults) {
        if (expectedResults != null) {
            Assert.assertEquals(results, expectedResults, "Data Doesn't match");
        }
        return results;
    }
}
