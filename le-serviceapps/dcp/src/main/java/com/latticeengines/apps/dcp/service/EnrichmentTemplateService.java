package com.latticeengines.apps.dcp.service;

import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

public interface EnrichmentTemplateService {

    EnrichmentTemplate create(String layoutId, String templateName);
}
