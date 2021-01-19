package com.latticeengines.pls.service.dcp;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

public interface EnrichmentTemplateService {

    ResponseDocument<String> createEnrichmentTemplate(String layoutId, String templateName);

    ResponseDocument<String> createEnrichmentTemplate(EnrichmentTemplate enrichmentTemplate);

}
