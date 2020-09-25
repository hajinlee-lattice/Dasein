package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutOperationResult;

public interface EnrichmentLayoutService {

    EnrichmentLayoutOperationResult create(EnrichmentLayout enrichmentLayout);

    EnrichmentLayoutOperationResult update(EnrichmentLayout enrichmentLayout);

    EnrichmentLayout findByLayoutId (String layoutId);

    EnrichmentLayout findBySourceId (String sourceId);

    void delete(String layoutId);

    void delete(EnrichmentLayout enrichmentLayout);

    List<EnrichmentLayout> getAll(String customerSpace);


}
