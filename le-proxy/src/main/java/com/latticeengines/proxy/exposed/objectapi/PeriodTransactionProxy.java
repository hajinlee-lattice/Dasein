package com.latticeengines.proxy.exposed.objectapi;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.DataCollection.Version;
import com.latticeengines.domain.exposed.metadata.transaction.ProductType;
import com.latticeengines.domain.exposed.ulysses.PeriodTransaction;
import com.latticeengines.domain.exposed.ulysses.ProductHierarchy;

public interface PeriodTransactionProxy {

    List<PeriodTransaction> getPeriodTransactionByAccountId(String customerSpace, String accountId, String periodName,
            Version version, ProductType productType);

    List<PeriodTransaction> getPeriodTransactionForSegmentAccount(String customerSpace, String spendanalyticssegment,
            String periodName);

    List<ProductHierarchy> getProductHierarchy(String customerSpace, DataCollection.Version version);

}
