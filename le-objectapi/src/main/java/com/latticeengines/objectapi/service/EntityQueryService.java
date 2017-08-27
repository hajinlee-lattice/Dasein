package com.latticeengines.objectapi.service;

import java.util.Map;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public interface EntityQueryService {

    long getCount(FrontEndQuery frontEndQuery);

    DataPage getData(FrontEndQuery frontEndQuery);

    Map<String, Long> getRatingCount(FrontEndQuery frontEndQuery);

}
