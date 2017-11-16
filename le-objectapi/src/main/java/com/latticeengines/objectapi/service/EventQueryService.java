package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public interface EventQueryService {

    DataPage getScoringTuples(FrontEndQuery frontEndQuery);

    DataPage getTrainingTuples(FrontEndQuery frontEndQuery);

    DataPage getEventTuples(FrontEndQuery frontEndQuery);
}
