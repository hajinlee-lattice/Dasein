package com.latticeengines.propdata.engine.transformation.service;

import com.latticeengines.domain.exposed.datacloud.transformation.ExternalEnrichRequest;

public interface ExternalEnrichService {

    void enrich(ExternalEnrichRequest request);

}
