package com.latticeengines.proxy.exposed.objectapi;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;

public interface PeriodTransactionProxy {

    List<PeriodTransaction> getPeriodTransactionsByAccountId(String customerSpace, String accountId, String periodName,
            ProductType productType);

    List<PeriodTransaction> getPeriodTransactionsForSegmentAccounts(String customerSpace, String spendanalyticssegment,
            String periodName);

    List<ProductHierarchy> getProductHierarchy(String customerSpace, DataCollection.Version version);

    DataPage getAllSpendAnalyticsSegments(String customerSpace);

    List<String> getFinalAndFirstTransactionDate(String customerSpace);
}
