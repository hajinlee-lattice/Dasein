package com.latticeengines.apps.dcp.service;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

public interface EnrichmentTemplateService {

    ResponseDocument<String> create(String layoutId, String templateName);

    ResponseDocument<String> create(EnrichmentTemplate enrichmentTemplate);
}
