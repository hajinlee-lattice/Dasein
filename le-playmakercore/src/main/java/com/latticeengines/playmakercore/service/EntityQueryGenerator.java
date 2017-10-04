package com.latticeengines.playmakercore.service;

import com.latticeengines.domain.exposed.query.DataRequest;
import com.latticeengines.domain.exposed.query.frontend.FrontEndQuery;

public interface EntityQueryGenerator {

    FrontEndQuery generateEntityQuery(Long start, DataRequest dataRequest);

    FrontEndQuery generateEntityQuery(Long start, Long offset, Long pageSize, DataRequest dataRequest);

}
