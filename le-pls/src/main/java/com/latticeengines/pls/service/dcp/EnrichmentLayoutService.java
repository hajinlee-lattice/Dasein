package com.latticeengines.pls.service.dcp;

import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutOperationResult;

public interface EnrichmentLayoutService {

    EnrichmentLayoutOperationResult create(String tenantId, EnrichmentLayout enrichmentLayout);

    EnrichmentLayout getEnrichmentLayoutBySourceId (String customerId, String sourceId);

    EnrichmentLayout getEnrichmentLayoutByLayoutId (String customerId, String layoutId);

    EnrichmentLayoutOperationResult update(String customerId, EnrichmentLayout enrichmentLayout);

    void delete(String customerId, String layoutId);
}
