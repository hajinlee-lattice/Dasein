package com.latticeengines.pls.service.dcp;

import java.util.List;

import com.latticeengines.domain.exposed.ResponseDocument;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplate;
import com.latticeengines.domain.exposed.dcp.EnrichmentTemplateSummary;

public interface EnrichmentTemplateService {

    ResponseDocument<EnrichmentTemplateSummary> createEnrichmentTemplate(String layoutId, String templateName);

    ResponseDocument<EnrichmentTemplateSummary> createEnrichmentTemplate(EnrichmentTemplate enrichmentTemplate);

    List<EnrichmentTemplateSummary> getEnrichmentTemplates(String domain, String recordType, Boolean includeArchived,
            String createdBy);

    EnrichmentTemplateSummary getEnrichmentTemplate(String customerSpace, String templateId);
}
