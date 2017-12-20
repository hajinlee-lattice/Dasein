package com.latticeengines.objectapi.service;

import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public interface EventQueryService {
    long getScoringCount(EventFrontEndQuery frontEndQuery);

    long getTrainingCount(EventFrontEndQuery frontEndQuery);

    long getEventCount(EventFrontEndQuery frontEndQuery);

    DataPage getScoringTuples(EventFrontEndQuery frontEndQuery);

    DataPage getTrainingTuples(EventFrontEndQuery frontEndQuery);

    DataPage getEventTuples(EventFrontEndQuery frontEndQuery);
}
