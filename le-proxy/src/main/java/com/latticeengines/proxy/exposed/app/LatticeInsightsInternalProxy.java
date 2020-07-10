package com.latticeengines.proxy.exposed.app;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttributesOperationMap;

public interface LatticeInsightsInternalProxy {

    List<String> getLeadEnrichmentCategories(CustomerSpace customerSpace);

    List<String> getLeadEnrichmentSubcategories(CustomerSpace customerSpace, String category);

    List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                              String attributeDisplayNameFilter, Category category, Boolean onlySelectedAttributes);

    List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                              String attributeDisplayNameFilter, Category category, Boolean onlySelectedAttributes, Boolean considerInternalAttributes);

    List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                              String attributeDisplayNameFilter, Category category, String subcategory, //
                                                              Boolean onlySelectedAttributes, Boolean considerInternalAttributes);

    List<LeadEnrichmentAttribute> getLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                                              String attributeDisplayNameFilter, Category category, String subcategory, //
                                                              Boolean onlySelectedAttributes, Integer offset, Integer max, Boolean considerInternalAttributes);

    Integer getSelectedAttributeCount(CustomerSpace customerSpace, Boolean considerInternalAttributes);

    Integer getSelectedAttributePremiumCount(CustomerSpace customerSpace, Boolean considerInternalAttributes);

    int getLeadEnrichmentAttributesCount(CustomerSpace customerSpace, String attributeDisplayNameFilter,
                                         Category category, String subcategory, Boolean onlySelectedAttributes, Boolean considerInternalAttributes);

    Map<String, Integer> getPremiumAttributesLimitation(CustomerSpace customerSpace);

    List<LeadEnrichmentAttribute> getAllLeadEnrichmentAttributes();

    void saveLeadEnrichmentAttributes(CustomerSpace customerSpace, //
                                      LeadEnrichmentAttributesOperationMap attributes);
}
