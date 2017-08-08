package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.Query;

public interface AccountQueryService {

    Query generateAccountQuery(String start, DataRequest dataRequest);

    Query generateAccountQuery(String start, Long offset, Long pageSize, DataRequest dataRequest);
}
