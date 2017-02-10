package com.latticeengines.app.exposed.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.collections.Closure;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.app.exposed.entitymanager.AttributeCustomizationPropertyEntityMgr;
import com.latticeengines.app.exposed.entitymanager.CategoryCustomizationPropertyEntityMgr;
import com.latticeengines.app.exposed.service.AttributeCustomizationService;
import com.latticeengines.app.exposed.service.AttributeService;
import com.latticeengines.common.exposed.util.DatabaseUtils;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.pls.AttributeCustomizationProperty;
import com.latticeengines.domain.exposed.pls.AttributeUseCase;
import com.latticeengines.domain.exposed.pls.CategoryCustomizationProperty;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.security.exposed.util.MultiTenantContext;

@Component("attributeCustomizationService")
public class AttributeCustomizationServiceImpl implements AttributeCustomizationService {
    private static final Logger log = Logger.getLogger(AttributeCustomizationServiceImpl.class);

    @Autowired
    private AttributeCustomizationPropertyEntityMgr attributeCustomizationPropertyEntityMgr;

    @Autowired
    private CategoryCustomizationPropertyEntityMgr categoryCustomizationPropertyEntityMgr;

    @Autowired
    private AttributeService attributeService;

    @Override
    public void save(String name, AttributeUseCase useCase, String propertyName, String value) {
        log.info(String.format("Customizing attribute %s for tenant %s and use case %s with flags %s:%s", name,
                MultiTenantContext.getCustomerSpace(), useCase, propertyName, value));
        LeadEnrichmentAttribute enrichmentAttr = attributeService.getAttribute(name);
        if (enrichmentAttr == null) {
            throw new LedpException(LedpCode.LEDP_36001, new String[] { name });
        }

        AttributeCustomizationProperty customization = new AttributeCustomizationProperty();
        customization.setName(name);
        customization.setUseCase(useCase);
        customization.setPropertyName(propertyName);
        customization.setPropertyValue(value);
        customization
                .setCategoryName(String.format("%s.%s", enrichmentAttr.getCategory(), enrichmentAttr.getSubcategory()));

        DatabaseUtils.retry("saveAttributeProperties", new Closure() {
            @Override
            public void execute(Object input) {
                attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);
            }
        });

    }

    @Override
    public String retrieve(String name, AttributeUseCase useCase, String propertyName) {
        LeadEnrichmentAttribute enrichmentAttr = attributeService.getAttribute(name);
        if (enrichmentAttr == null) {
            throw new LedpException(LedpCode.LEDP_36001, new String[] { name });
        }
        AttributeCustomizationProperty attrPropertyCustomization = attributeCustomizationPropertyEntityMgr.find(name, useCase,
                propertyName);
        if (attrPropertyCustomization != null) {
            return attrPropertyCustomization.getPropertyValue();
        }
        String category = enrichmentAttr.getCategory();
        String subCategory = enrichmentAttr.getSubcategory();
        CategoryCustomizationProperty categoryPropertyCustomization = categoryCustomizationPropertyEntityMgr
                .find(useCase, String.format("%s.%s", category, subCategory), propertyName);
        if (categoryPropertyCustomization != null) {
            return categoryPropertyCustomization.getPropertyValue();
        }
        categoryPropertyCustomization = categoryCustomizationPropertyEntityMgr.find(useCase, category,
                propertyName);
        if (categoryPropertyCustomization != null) {
            return categoryPropertyCustomization.getPropertyValue();
        }
        return Boolean.FALSE.toString();
    }

    @Override
    public void addFlags(List<LeadEnrichmentAttribute> attributes) {
        List<AttributeCustomizationProperty> allCustomizations = attributeCustomizationPropertyEntityMgr.findAll();
        Map<String, List<AttributeCustomizationProperty>> customizationMap = new HashMap<>();
        for (AttributeCustomizationProperty customization : allCustomizations) {
            List<AttributeCustomizationProperty> list = customizationMap.get(customization.getName());
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(customization);
            customizationMap.put(customization.getName(), list);
        }

        for (LeadEnrichmentAttribute attribute : attributes) {
            List<AttributeCustomizationProperty> customizations = customizationMap.get(attribute.getFieldName());
            for (AttributeUseCase useCase : AttributeUseCase.values()) {
                if (customizations != null) {

                    List<AttributeCustomizationProperty> filteredCustomizations = customizations.stream()
                            .filter(customization -> customization.getUseCase() == useCase)
                            .collect(Collectors.toList());

                    if (!filteredCustomizations.isEmpty()) {
                        ObjectNode node = JsonUtils.createObjectNode();
                        for (AttributeCustomizationProperty customization : filteredCustomizations) {
                            node.put(customization.getPropertyName(), customization.getPropertyValue());
                        }

                        Map<AttributeUseCase, String> flagsMap = new HashMap<>();
                        flagsMap.put(useCase, node.toString());
                        attribute.setAttributeFlagsMap(flagsMap);
                    }
                }
            }
        }
    }

    @Override
    public void saveCategory(Category category, AttributeUseCase useCase, String propertyName, String value) {
//        List<AttributePropertyCustomization> found = attributeCustomizationEntityMgr.find(category, useCase,
//                propertyName);
//        if (!found.isEmpty()) {
//            for (AttributePropertyCustomization attr : found) {
//                attributeCustomizationEntityMgr.delete(attr);
//            }
//        }
//        List<LeadEnrichmentAttribute> leadAttrs = attributeService.getAttributesBaseOnCategory(category);
//        for (LeadEnrichmentAttribute attr : leadAttrs) {
//            save(attr.getFieldName(), useCase, propertyName, value);
//        }
    }

    @Override
    public void saveSubCategory(Category category, String subcategoryName, AttributeUseCase useCase,
            String propertyName, String value) {
        

    }
}
