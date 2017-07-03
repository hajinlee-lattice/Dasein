package com.latticeengines.network.exposed.objectapi;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.DataRequest;

public interface AccountInterface {
    DataPage getAccounts(String customerSpace, String start, int offset, int pageSize, boolean hasSfdcAccountId,
            DataRequest dataRequest);
}
