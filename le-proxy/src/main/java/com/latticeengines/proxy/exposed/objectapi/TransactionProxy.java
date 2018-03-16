package com.latticeengines.proxy.exposed.objectapi;

import com.latticeengines.domain.exposed.metadata.DataCollection;

public interface TransactionProxy {
    String getMaxTransactionDate(String customerSpace, DataCollection.Version version);
}
