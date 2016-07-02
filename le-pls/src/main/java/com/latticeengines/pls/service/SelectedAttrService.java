package com.latticeengines.pls.service;

import java.util.List;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SelectedAttrService {

    void save(LeadEnrichmentAttributesOperationMap attributes, Tenant tenant, Integer premiumAttributeLimitation);

    List<LeadEnrichmentAttribute> getAttributes(Tenant tenant, String attributeDisplayNameFilter, Category category,
            Boolean onlySelectedAttributes);

    Integer getSelectedAttributeCount(Tenant tenant);

    Integer getSelectedAttributePremiumCount(Tenant tenant);

    Integer getPremiumAttributesLimitation(Tenant tenant);

}
