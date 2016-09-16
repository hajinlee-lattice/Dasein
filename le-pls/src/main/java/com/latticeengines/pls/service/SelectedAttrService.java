package com.latticeengines.pls.service;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SelectedAttrService {

    void save(LeadEnrichmentAttributesOperationMap attributes, Tenant tenant, Map<String, Integer> limitationMap);

    List<LeadEnrichmentAttribute> getAttributes(Tenant tenant, String attributeDisplayNameFilter, Category category,
            Boolean onlySelectedAttributes, Integer offset, Integer max);

    int getAttributesCount(Tenant tenant, String attributeDisplayNameFilter, Category categoryEnum,
            Boolean onlySelectedAttributes);

    Integer getSelectedAttributeCount(Tenant tenant);

    Integer getSelectedAttributePremiumCount(Tenant tenant);

    Map<String, Integer> getPremiumAttributesLimitation(Tenant tenant);

    void downloadAttributes(HttpServletRequest request, HttpServletResponse response, String mimeType, String fileName,
            Tenant tenant, Boolean isSelected);

}
