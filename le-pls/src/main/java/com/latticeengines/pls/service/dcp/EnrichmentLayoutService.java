package com.latticeengines.pls.service.dcp;

import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutOperationResult;

public interface EnrichmentLayoutService {

    EnrichmentLayoutOperationResult create(String tenantId, EnrichmentLayout enrichmentLayout);

    EnrichmentLayoutDetail getEnrichmentLayoutBySourceId (String customerId, String sourceId);

    EnrichmentLayoutDetail getEnrichmentLayoutByLayoutId (String customerId, String layoutId);

    EnrichmentLayoutOperationResult update(String customerId, EnrichmentLayout enrichmentLayout);

    EnrichmentLayoutOperationResult delete(String customerId, String layoutId);
}
