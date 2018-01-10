package com.latticeengines.scoringapi.enrich;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;

public interface EnrichRequestProcessor {

    EnrichResponse process(CustomerSpace space, EnrichRequest request, boolean enrichInternalAttributes,
            String requestId);
}
