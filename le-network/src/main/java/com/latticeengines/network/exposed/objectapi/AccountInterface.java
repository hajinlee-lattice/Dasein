package com.latticeengines.network.exposed.objectapi;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;

public interface AccountInterface {
    DataPage getAccounts(String customerSpace, String start, Integer offset, Integer pageSize, Boolean hasSfdcAccountId,
            DataRequest dataRequest);
}
