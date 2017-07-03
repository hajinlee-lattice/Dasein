package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.Query;

public interface AccountQueryService {

    Query generateAccountQuery(String start, int offset, int pageSize, boolean hasSfdcAccountId,
            DataRequest dataRequest);
}
