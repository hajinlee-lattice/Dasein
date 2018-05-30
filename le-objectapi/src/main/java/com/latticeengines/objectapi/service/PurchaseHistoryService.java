package com.latticeengines.objectapi.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;

public interface PurchaseHistoryService {

    List<PeriodTransaction> getPeriodTransactionByAccountId(String accountId, String periodName, Version version,
            ProductType productType);

    List<PeriodTransaction> getPeriodTransactionForSegmentAccount(String segment, String periodName,
            ProductType productType);

    List<ProductHierarchy> getProductHierarchy(DataCollection.Version version);

    List<String> getAllSegments();

}
