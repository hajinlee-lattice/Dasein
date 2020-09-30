package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;
import com.latticeengines.domain.exposed.dcp.EnrichmentLayoutDetail;

public interface EnrichmentLayoutService {

    ResponseDocument<String> create(String customerSpace, EnrichmentLayout enrichmentLayout);

    ResponseDocument<String> update(String customerSpace, EnrichmentLayout enrichmentLayout);

    EnrichmentLayout findByLayoutId(String customerSpace, String layoutId);

    EnrichmentLayout findBySourceId(String customerSpace, String sourceId);

    EnrichmentLayoutDetail findEnrichmentLayoutDetailByLayoutId(String customerSpace, String layoutId);

    EnrichmentLayoutDetail findEnrichmentLayoutDetailBySourceId(String customerSpace, String layoutId);

    void deleteLayoutByLayoutId(String customerSpace, String layoutId);

    void deleteLayout(EnrichmentLayout enrichmentLayout);

    List<EnrichmentLayoutDetail> getAll(String customerSpace, int pageIndex, int pageSize);
}
