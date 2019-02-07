package com.latticeengines.objectapi.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;

public interface PurchaseHistoryService {

    List<PeriodTransaction> getPeriodTransactionsByAccountId(String accountId, String periodName,
            ProductType productType);

    List<PeriodTransaction> getPeriodTransactionsForSegmentAccounts(String spendAnalyticsSegment, String periodName,
            ProductType productType);

    List<ProductHierarchy> getProductHierarchy(DataCollection.Version version);

    DataPage getAllSpendAnalyticsSegments();

    List<String> getFinalAndFirstTransactionDate();

}
