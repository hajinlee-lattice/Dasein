package com.latticeengines.pls.service.dcp;

import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;

public interface EnrichmentTemplateService {

    EnrichmentTemplate createEnrichmentTemplate(String customerSpace, String layoutId, String templateName);

}
