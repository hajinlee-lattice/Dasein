package com.latticeengines.pls.service.dcp;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;

public interface EnrichmentLayoutService {

    ResponseDocument<String> create(String tenantId, EnrichmentLayout enrichmentLayout);

    EnrichmentLayoutDetail getEnrichmentLayoutBySourceId (String customerId, String sourceId);

    EnrichmentLayoutDetail getEnrichmentLayoutByLayoutId (String customerId, String layoutId);

    ResponseDocument<String> update(String customerId, EnrichmentLayout enrichmentLayout);

    ResponseDocument<String> deleteByLayoutId(String customerId, String layoutId);

    ResponseDocument<String> deleteBySourceId(String customerId, String sourceId);
}
