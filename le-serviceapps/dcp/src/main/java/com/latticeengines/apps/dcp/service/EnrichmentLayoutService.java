package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.EnrichmentLayout;

public interface EnrichmentLayoutService {

    void create(EnrichmentLayout enrichmentLayout);

    boolean update(EnrichmentLayout enrichmentLayout);

    EnrichmentLayout findByLayoutId (String layoutId);

    EnrichmentLayout findBySourceId (String sourceId);

    void delete (String layoutId);

    void delete(EnrichmentLayout enrichmentLayout);

    boolean validate (EnrichmentLayout enrichmentLayout);

    List<EnrichmentLayout> getAll();


}
