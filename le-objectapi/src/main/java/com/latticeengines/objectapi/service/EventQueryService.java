package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public interface EventQueryService {

    DataPage getSegmentTuple(FrontEndQuery frontEndQuery);

    DataPage getTrainingTuple(FrontEndQuery frontEndQuery);

    DataPage getEventTuple(FrontEndQuery frontEndQuery);
}
