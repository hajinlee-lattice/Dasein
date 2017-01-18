package com.latticeengines.app.exposed.service;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;
import com.latticeengines.domain.exposed.security.Tenant;

public interface SelectedAttrService {

    void save(LeadEnrichmentAttributesOperationMap attributes, Tenant tenant, Map<String, Integer> limitationMap,
              Boolean considerInternalAttributes);

    List<LeadEnrichmentAttribute> getAttributes(Tenant tenant, String attributeDisplayNameFilter, Category category,
                                                String subcategory, Boolean onlySelectedAttributes, Integer offset, Integer max,
                                                Boolean considerInternalAttributes);

    List<LeadEnrichmentAttribute> getAllAttributes();

    int getAttributesCount(Tenant tenant, String attributeDisplayNameFilter, Category categoryEnum, String subcategory,
                           Boolean onlySelectedAttributes, Boolean considerInternalAttributes);

    Integer getSelectedAttributeCount(Tenant tenant, Boolean considerInternalAttributes);

    Integer getSelectedAttributePremiumCount(Tenant tenant, Boolean considerInternalAttributes);

    Map<String, Integer> getPremiumAttributesLimitation(Tenant tenant);

    void downloadAttributes(HttpServletRequest request, HttpServletResponse response, String mimeType, String fileName,
                            Tenant tenant, Boolean isSelected, Boolean considerInternalAttributes);

}
