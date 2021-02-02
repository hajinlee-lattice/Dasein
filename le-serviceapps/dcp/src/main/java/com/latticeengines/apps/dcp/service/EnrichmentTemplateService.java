package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;
import com.latticeengines.domain.exposed.dcp.ListEnrichmentTemplateRequest;

public interface EnrichmentTemplateService {

    ResponseDocument<String> create(String customerSpace, String layoutId, String templateName);

    ResponseDocument<String> create(EnrichmentTemplate enrichmentTemplate);

    List<EnrichmentTemplateSummary> getEnrichmentTemplates(ListEnrichmentTemplateRequest listEnrichmentTemplateRequest);
}
