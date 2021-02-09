package com.latticeengines.apps.dcp.service;

import java.util.List;

import com.latticeengines.domain.exposed.dcp.CreateEnrichmentTemplateRequest;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;
import com.latticeengines.domain.exposed.dcp.ListEnrichmentTemplateRequest;

public interface EnrichmentTemplateService {

    EnrichmentTemplateSummary create(String customerSpace, CreateEnrichmentTemplateRequest request);

    EnrichmentTemplateSummary create(EnrichmentTemplate enrichmentTemplate);

    List<EnrichmentTemplateSummary> listEnrichmentTemplates(ListEnrichmentTemplateRequest listEnrichmentTemplateRequest);

    EnrichmentTemplateSummary getEnrichmentTemplate(String templateId);
}
