package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.Query;

public interface AccountQueryService {

    Query generateAccountQuery(String start, Integer offset, Integer pageSize, Boolean hasSfdcAccountId,
            DataRequest dataRequest);
}
