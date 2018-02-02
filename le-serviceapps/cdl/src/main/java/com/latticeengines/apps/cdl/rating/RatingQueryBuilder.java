package com.latticeengines.apps.cdl.rating;

import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.RatingModel;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;

public interface RatingQueryBuilder {
    EventFrontEndQuery build();
}
