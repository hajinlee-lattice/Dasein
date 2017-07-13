package com.latticeengines.network.exposed.objectapi;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;

public interface AccountInterface {
    long getAccountsCount(String customerSpace, String start, DataRequest dataRequest);

    DataPage getAccounts(String customerSpace, String start, Integer offset, Integer pageSize, DataRequest dataRequest);
}
