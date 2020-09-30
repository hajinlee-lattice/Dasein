package com.latticeengines.apps.dcp.service;

import java.util.List;

import org.springframework.data.domain.Pageable;

import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutOperationResult;

public interface EnrichmentLayoutService {

    EnrichmentLayoutOperationResult create(String customerSpace, EnrichmentLayout enrichmentLayout);

    EnrichmentLayoutOperationResult update(String customerSpace, EnrichmentLayout enrichmentLayout);

    EnrichmentLayout findByLayoutId(String customerSpace, String layoutId);

    EnrichmentLayout findBySourceId(String customerSpace, String sourceId);

    EnrichmentLayoutDetail findEnrichmentLayoutDetailByLayoutId(String customerSpace, String layoutId);

    EnrichmentLayoutDetail findEnrichmentLayoutDetailBySourceId(String customerSpace, String layoutId);

    void deleteLayoutByLayoutId(String customerSpace, String layoutId);

    void deleteLayout(EnrichmentLayout enrichmentLayout);

    void hardDeleteLayoutByLayoutId(String customerSpace, String layoutId);

    void hardDeleteLayout(EnrichmentLayout enrichmentLayout);

    List<EnrichmentLayoutDetail> getAll(String customerSpace, Boolean includeArchived, int pageIndex, int pageSize);
}
