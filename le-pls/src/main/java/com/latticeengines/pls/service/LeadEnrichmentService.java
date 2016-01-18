package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;

public interface LeadEnrichmentService {

    List<LeadEnrichmentAttribute> getAvariableAttributes();

    List<LeadEnrichmentAttribute> getSavedAttributes();
}
