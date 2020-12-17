package com.latticeengines.apps.dcp.service;

import com.latticeengines.domain.exposed.ResponseDocument;

public interface EnrichmentTemplateService {

    ResponseDocument<String> create(String layoutId, String templateName);
}
