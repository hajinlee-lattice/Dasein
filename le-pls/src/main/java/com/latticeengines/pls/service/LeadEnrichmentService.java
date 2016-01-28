package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.security.Tenant;

public interface LeadEnrichmentService {

    List<LeadEnrichmentAttribute> getAvailableAttributes();

    List<LeadEnrichmentAttribute> getAttributes(Tenant tenant);

    public void saveAttributes(Tenant tenant, List<LeadEnrichmentAttribute> attributes);
}
