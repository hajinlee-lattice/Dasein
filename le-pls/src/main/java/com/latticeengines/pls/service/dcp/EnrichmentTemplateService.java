package com.latticeengines.pls.service.dcp;

import com.latticeengines.domain.exposed.ResponseDocument;

public interface EnrichmentTemplateService {

    ResponseDocument<String> createEnrichmentTemplate(String customerSpace, String layoutId, String templateName);

}
