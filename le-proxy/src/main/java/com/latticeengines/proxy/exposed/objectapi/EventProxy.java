package com.latticeengines.proxy.exposed.objectapi;

import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.DataPage;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public interface EventProxy {

    // cached
    Long getScoringCount(String customerSpace, EventFrontEndQuery frontEndQuery);

    // cached
    Long getTrainingCount(String customerSpace, EventFrontEndQuery frontEndQuery);

    // cached
    Long getEventCount(String customerSpace, EventFrontEndQuery frontEndQuery);

    Long getScoringCount(String customerSpace, EventFrontEndQuery frontEndQuery,
                         DataCollection.Version version);

    Long getTrainingCount(String customerSpace, EventFrontEndQuery frontEndQuery,
                          DataCollection.Version version);

    Long getEventCount(String customerSpace, EventFrontEndQuery frontEndQuery,
                       DataCollection.Version version);

    DataPage getScoringTuples(String customerSpace, EventFrontEndQuery frontEndQuery,
            DataCollection.Version version);

    DataPage getTrainingTuples(String customerSpace, EventFrontEndQuery frontEndQuery,
            DataCollection.Version version);

    DataPage getEventTuples(String customerSpace, EventFrontEndQuery frontEndQuery,
            DataCollection.Version version);

}
