package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

import java.util.Map;

public interface EntityQueryService {

    long getCount(BusinessEntity entity, FrontEndQuery frontEndQuery);

    DataPage getData(BusinessEntity entity, FrontEndQuery frontEndQuery);

    Map<String, Long> getRatingCount(BusinessEntity entity, FrontEndQuery frontEndQuery);

}
