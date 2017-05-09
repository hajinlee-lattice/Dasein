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
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.fasterxml.jackson.databind.JsonNode;
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
import com.latticeengines.domain.exposed.pls.HasAttributeCustomizations;
import com.latticeengines.domain.exposed.pls.LeadEnrichmentAttribute;
import com.latticeengines.domain.exposed.util.CategoryNameUtils;
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
        customization.setCategoryName(CategoryNameUtils.getCategoryName(enrichmentAttr.getCategory(), //
                enrichmentAttr.getSubcategory()));

        DatabaseUtils.retry("saveAttributeProperties", new Closure() {
            @Override
            public void execute(Object input) {
                attributeCustomizationPropertyEntityMgr.createOrUpdate(customization);
            }
        });

    }

    @Override
    public void save(String name, AttributeUseCase useCase, Map<String, String> properties) {
        log.info(String.format("Customizing attribute %s for tenant %s and use case %s with flags %s", name,
                MultiTenantContext.getCustomerSpace(), useCase, properties));
        LeadEnrichmentAttribute enrichmentAttr = attributeService.getAttribute(name);
        if (enrichmentAttr == null) {
            throw new LedpException(LedpCode.LEDP_36001, new String[] { name });
        }

        for (String propertyName : properties.keySet()) {
            save(name, useCase, propertyName, properties.get(propertyName));
        }
    }

    @Override
    public String retrieve(String name, AttributeUseCase useCase, String propertyName) {
        LeadEnrichmentAttribute enrichmentAttr = attributeService.getAttribute(name);
        if (enrichmentAttr == null) {
            throw new LedpException(LedpCode.LEDP_36001, new String[] { name });
        }
        AttributeCustomizationProperty attrPropertyCustomization = attributeCustomizationPropertyEntityMgr.find(name,
                useCase, propertyName);
        if (attrPropertyCustomization != null) {
            return attrPropertyCustomization.getPropertyValue();
        }
        String category = enrichmentAttr.getCategory();
        String subCategory = enrichmentAttr.getSubcategory();
        CategoryCustomizationProperty categoryPropertyCustomization = categoryCustomizationPropertyEntityMgr.find(
                useCase, CategoryNameUtils.getCategoryName(category, subCategory), propertyName);
        if (categoryPropertyCustomization != null) {
            return categoryPropertyCustomization.getPropertyValue();
        }
        categoryPropertyCustomization = categoryCustomizationPropertyEntityMgr.find(useCase, category, propertyName);
        if (categoryPropertyCustomization != null) {
            return categoryPropertyCustomization.getPropertyValue();
        }
        return Boolean.FALSE.toString();
    }

    @Override
    public void addFlags(List<HasAttributeCustomizations> attributes) {
        List<AttributeCustomizationProperty> allAttrCustomizations = attributeCustomizationPropertyEntityMgr.findAll();
        List<CategoryCustomizationProperty> allCategoryCustomizations = categoryCustomizationPropertyEntityMgr
                .findAll();
        Map<String, List<AttributeCustomizationProperty>> attrCustomizationMap = new HashMap<>();
        for (AttributeCustomizationProperty customization : allAttrCustomizations) {
            List<AttributeCustomizationProperty> list = attrCustomizationMap.get(customization.getName());
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(customization);
            attrCustomizationMap.put(customization.getName(), list);
        }

        Map<String, List<CategoryCustomizationProperty>> categoryCustomizationMap = new HashMap<>();
        for (CategoryCustomizationProperty customization : allCategoryCustomizations) {
            List<CategoryCustomizationProperty> list = categoryCustomizationMap.get(customization.getCategoryName());
            if (list == null) {
                list = new ArrayList<>();
            }
            list.add(customization);
            categoryCustomizationMap.put(customization.getCategoryName(), list);
        }

        for (HasAttributeCustomizations attribute : attributes) {
            for (AttributeUseCase useCase : AttributeUseCase.values()) {
                if (setAttributePropertyMap(attrCustomizationMap, useCase, attribute)) {
                    continue;
                }
                String categoryName = CategoryNameUtils.getCategoryName(attribute.getCategoryAsString(),
                        attribute.getSubcategory());
                if (setAttributePropertyMap(categoryCustomizationMap, categoryName, useCase, attribute)) {
                    continue;
                }
                setAttributePropertyMap(categoryCustomizationMap, attribute.getCategoryAsString(), useCase, attribute);
            }
        }
    }

    private boolean setAttributePropertyMap(Map<String, List<AttributeCustomizationProperty>> attrCustomizationMap,
            AttributeUseCase useCase, HasAttributeCustomizations attribute) {
        List<AttributeCustomizationProperty> attrCustomizations = attrCustomizationMap.get(attribute.getColumnId());
        if (attrCustomizations == null) {
            return false;
        }
        List<AttributeCustomizationProperty> filteredAttrCustomizations = attrCustomizations.stream()
                .filter(customization -> customization.getUseCase() == useCase).collect(Collectors.toList());
        if (filteredAttrCustomizations.isEmpty()) {
            return false;
        }
        ObjectNode node = JsonUtils.createObjectNode();
        for (AttributeCustomizationProperty customization : filteredAttrCustomizations) {
            node.put(customization.getPropertyName(), Boolean.parseBoolean(customization.getPropertyValue()));
        }
        Map<AttributeUseCase, JsonNode> flagsMap = new HashMap<>();
        flagsMap.put(useCase, node);
        attribute.setAttributeFlagsMap(flagsMap);
        return true;
    }

    private boolean setAttributePropertyMap(Map<String, List<CategoryCustomizationProperty>> categoryCustomizationMap,
            String categoryName, AttributeUseCase useCase, HasAttributeCustomizations attribute) {
        List<CategoryCustomizationProperty> categoryCustomizations = categoryCustomizationMap.get(categoryName);
        if (categoryCustomizations == null) {
            return false;
        }
        List<CategoryCustomizationProperty> filteredCategoryCustomizations = categoryCustomizations.stream()
                .filter(customization -> customization.getUseCase() == useCase).collect(Collectors.toList());
        if (filteredCategoryCustomizations.isEmpty()) {
            return false;
        }
        ObjectNode node = JsonUtils.createObjectNode();
        for (CategoryCustomizationProperty customization : filteredCategoryCustomizations) {
            node.put(customization.getPropertyName(), Boolean.parseBoolean(customization.getPropertyValue()));
        }
        Map<AttributeUseCase, JsonNode> flagsMap = new HashMap<>();
        flagsMap.put(useCase, node);
        attribute.setAttributeFlagsMap(flagsMap);
        return true;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public void saveCategory(Category category, AttributeUseCase useCase, String propertyName, String value) {
        attributeCustomizationPropertyEntityMgr.deleteCategory(category, useCase, propertyName);
        categoryCustomizationPropertyEntityMgr.deleteSubcategories(category, useCase, propertyName);
        CategoryCustomizationProperty categoryCustomizationProperty = new CategoryCustomizationProperty();
        categoryCustomizationProperty.setUseCase(useCase);
        categoryCustomizationProperty.setCategoryName(category.getName());
        categoryCustomizationProperty.setPropertyName(propertyName);
        categoryCustomizationProperty.setPropertyValue(value);
        DatabaseUtils.retry("saveCategoryProperties", new Closure() {
            @Override
            public void execute(Object input) {
                categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomizationProperty);
            }
        });
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, isolation = Isolation.READ_COMMITTED)
    public void saveSubcategory(Category category, String subcategoryName, AttributeUseCase useCase,
            String propertyName, String value) {
        String categoryName = CategoryNameUtils.getCategoryName(category.getName(), subcategoryName);
        attributeCustomizationPropertyEntityMgr.deleteSubcategory(categoryName, useCase, propertyName);
        CategoryCustomizationProperty categoryCustomizationProperty = new CategoryCustomizationProperty();
        categoryCustomizationProperty.setUseCase(useCase);
        categoryCustomizationProperty.setCategoryName(categoryName);
        categoryCustomizationProperty.setPropertyName(propertyName);
        categoryCustomizationProperty.setPropertyValue(value);
        DatabaseUtils.retry("saveCategoryProperties", new Closure() {
            @Override
            public void execute(Object input) {
                categoryCustomizationPropertyEntityMgr.createOrUpdate(categoryCustomizationProperty);
            }
        });
    }

    @Override
    public void saveCategory(Category category, AttributeUseCase useCase, Map<String, String> properties) {
        log.info(String.format("Saving properties for category %s, use case %s, properties: %s", category, useCase,
                properties));
        for (String propertyName : properties.keySet()) {
            saveCategory(category, useCase, propertyName, properties.get(propertyName));
        }
    }

    @Override
    public void saveSubcategory(Category category, String subcategoryName, AttributeUseCase useCase,
            Map<String, String> properties) {
        log.info(String.format("Saving properties for category %s, use case %s, properties: %s",
                CategoryNameUtils.getCategoryName(category.getName(), subcategoryName), useCase, properties));
        for (String propertyName : properties.keySet()) {
            saveSubcategory(category, subcategoryName, useCase, propertyName, properties.get(propertyName));
        }
    }
}
