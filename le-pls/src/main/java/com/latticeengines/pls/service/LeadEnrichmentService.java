package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.security.Tenant;

public interface LeadEnrichmentService {

    List<LeadEnrichmentAttribute> getAvailableAttributes();

    List<LeadEnrichmentAttribute> getAttributes(Tenant tenant);

    // Verify attributes' field name are exist in target system (SFDC/Eloqua/Maketo).
    // Returns a map of invalid fields: key is table name, value are non-existent fields' name.
    Map<String, List<String>> verifyAttributes(Tenant tenant, List<LeadEnrichmentAttribute> attributes);

    void saveAttributes(Tenant tenant, List<LeadEnrichmentAttribute> attributes);

    String getTemplateType(Tenant tenant);
}
