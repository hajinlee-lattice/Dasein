package com.latticeengines.network.exposed.scoringapi;

import com.latticeengines.domain.exposed.scoringapi.EnrichRequest;
import com.latticeengines.domain.exposed.scoringapi.EnrichResponse;

public interface ScoringApiEnrichInterface {

    EnrichResponse enrichRecord(EnrichRequest request, String tenantIdentifier, String credentialId);
}
