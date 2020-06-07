package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.common.exposed.validator.annotation.NotNull;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.AttributeSetResponse;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigProp;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigRequest;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfigUpdateMode;
import com.latticeengines.domain.exposed.util.ApsGeneratorUtils;
import com.latticeengines.domain.exposed.util.AttributeUtils;
import com.latticeengines.domain.exposed.util.CategoryUtils;

@Service("cdlAttrConfigService")
public class CDLAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    private static final Logger log = LoggerFactory.getLogger(CDLAttrConfigServiceImpl.class);

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private AttributeSetEntityMgr attributeSetEntityMgr;

    private static int ATTRIBUTE_SET_LIMITATION = 50;

    @Override
    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        String tenantId = MultiTenantContext.getShortTenantId();
        DataCollection.Version version = dataCollectionService.getActiveVersion(tenantId);
        return servingStoreService.getSystemMetadata(entity, version) //
                .sequential().collectList().block();
    }

    @Override
    protected List<ColumnMetadata> getSystemMetadata(Category category) {
        List<BusinessEntity> entities = CategoryUtils.getEntity(category);
        String tenantId = MultiTenantContext.getShortTenantId();
        DataCollection.Version version = dataCollectionService.getActiveVersion(tenantId);
        List<ColumnMetadata> systemMds = new ArrayList<>();
        entities.forEach(entity -> systemMds.addAll(servingStoreService.getSystemMetadata(entity, version) //
                .filter(cm -> category.equals(cm.getCategory())) //
                .sequential().collectList().block()));
        return systemMds;
    }

    @Override
    public List<AttrConfig> getRenderedList(BusinessEntity entity, boolean render) {
        String tenantId = MultiTenantContext.getShortTenantId();
        List<AttrConfig> renderedList;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            boolean entityMatchEnabled = batonService.isEntityMatchEnabled(MultiTenantContext.getCustomerSpace());
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntity(tenantId, entity);
            List<ColumnMetadata> columns = getSystemMetadata(entity);
            if (render) {
                renderedList = render(columns, customConfig, entityMatchEnabled);
            } else {
                renderedList = customConfig;
            }
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs", count);
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

    @Override
    public AttributeSet getAttributeSetByName(String name) {
        return attributeSetEntityMgr.findByName(name);
    }

    @Override
    public List<AttributeSet> getAttributeSets(boolean withAttributesMap) {
        if (withAttributesMap) {
            return attributeSetEntityMgr.findAllWithAttributesMap();
        } else {
            return attributeSetEntityMgr.findAll();
        }
    }

    private void validateAttributeSet(boolean createOrClone, AttributeSet attributeSet) {
        if (createOrClone) {
            List<AttributeSet> attributeSets = attributeSetEntityMgr.findAll();
            if (attributeSets.size() > ATTRIBUTE_SET_LIMITATION) {
                throw new LedpException(LedpCode.LEDP_40084, new String[]{String.valueOf(ATTRIBUTE_SET_LIMITATION)});
            }
            if (StringUtils.isBlank(attributeSet.getDisplayName())) {
                throw new LedpException(LedpCode.LEDP_40085, new String[]{});
            }
        }
        String displayName = attributeSet.getDisplayName();
        if (StringUtils.isNotEmpty(displayName)) {
            displayName = displayName.trim();
            attributeSet.setDisplayName(displayName);
            AttributeSet attributeSet2 = attributeSetEntityMgr.findByDisPlayName(displayName);
            if (attributeSet2 != null && !attributeSet2.getName().equals(attributeSet.getName())) {
                throw new LedpException(LedpCode.LEDP_40086, new String[]{displayName});
            }
        }
    }

    private Set<String> getAttributes(Map<String, Set<String>> attributesMap, Category category, BusinessEntity entity) {
        Set<String> attributes = attributesMap.get(category.name());
        if (Category.PRODUCT_SPEND.equals(category) && CollectionUtils.isNotEmpty(attributes)) {
            Set<String> result = new HashSet<>();
            for (String attrName : attributes) {
                if (ApsGeneratorUtils.isApsAttr(attrName)) {
                    if (BusinessEntity.AnalyticPurchaseState.equals(entity)) {
                        result.add(attrName);
                    }
                } else {
                    if (BusinessEntity.PurchaseHistory.equals(entity)) {
                        result.add(attrName);
                    }
                }
            }
            return result;
        }
        return attributes == null ? new HashSet<>() : attributes;
    }

    private AttrConfigRequest validateRequest(AttributeSet existingAttributeSet, @NotNull AttributeSet newAttributeSet) {
        Map<BusinessEntity, Set<String>> entitySetMap = new HashMap<>();
        AttrConfigRequest attrConfigRequest = new AttrConfigRequest();
        List<AttrConfig> attrConfigs = new ArrayList<>();
        attrConfigRequest.setAttrConfigs(attrConfigs);
        Map<String, Set<String>> newAttributesMap = newAttributeSet.getAttributesMap();
        if (newAttributesMap != null) {
            for (BusinessEntity entity : BusinessEntity.SEGMENT_ENTITIES) {
                Category category = CategoryUtils.getEntityCategory(entity);
                Set<String> existingAttributes = new HashSet<>();
                if (existingAttributeSet != null && existingAttributeSet.getAttributesMap() != null) {
                    existingAttributes = getAttributes(existingAttributeSet.getAttributesMap(), category, entity);
                }
                entitySetMap.put(entity, existingAttributes);
            }
            for (Map.Entry<String, Set<String>> entry : newAttributesMap.entrySet()) {
                Category category = Category.valueOf(entry.getKey());
                Set<String> newAttributes;
                if (existingAttributeSet != null && existingAttributeSet.getAttributesMap() != null) {
                    Set<String> existingAttributes = existingAttributeSet.getAttributesMap().get(category.name());
                    existingAttributes = existingAttributes != null ? existingAttributes : new HashSet<>();
                    newAttributes = newAttributesMap.get(category.name());
                    generateAttrConfigRequestByCategory(attrConfigs, existingAttributes, newAttributes, category);
                } else {
                    newAttributes = newAttributesMap.get(category.name());
                    generateAttrConfigRequestByCategory(attrConfigs, new HashSet<>(), newAttributes, category);
                }
            }
            attrConfigRequest = validateRequest(attrConfigRequest, AttrConfigUpdateMode.Usage, entitySetMap);
        }
        return attrConfigRequest;
    }

    private void generateAttrConfigRequestByCategory(List<AttrConfig> attrConfigs, Set<String> existingAttrs, Set<String> newAttrs, Category category) {
        if (newAttrs != null) {
            Set<String> selectedAttrs = newAttrs.stream().filter(attr -> !existingAttrs.contains(attr)).collect(Collectors.toSet());
            Set<String> unselectedAttrs = existingAttrs.stream().filter(attr -> !newAttrs.contains(attr)).collect(Collectors.toSet());
            generateAttrConfigRequestByCategory(attrConfigs, selectedAttrs, category, ColumnSelection.Predefined.Enrichment.name(), Boolean.TRUE);
            generateAttrConfigRequestByCategory(attrConfigs, unselectedAttrs, category, ColumnSelection.Predefined.Enrichment.name(), Boolean.FALSE);
        }
    }

    private void generateAttrConfigRequestByCategory(List<AttrConfig> attrConfigs, Set<String> attributes,
                                                             Category category, String property, Boolean selectThisAttr) {
        for (String attrName : attributes) {
            BusinessEntity entity = getEntity(category, attrName);
            generateAttrConfigRequestForUsage(attrConfigs, attrName, entity, selectThisAttr, property);
        }
    }

    private void generateAttrConfigRequestForUsage(List<AttrConfig> attrConfigs, String attrName,
                                                   BusinessEntity entity, Boolean selectThisAttr, String propertyName) {
        AttrConfig config = new AttrConfig();
        config.setAttrName(attrName);
        config.setEntity(entity);
        AttrConfigProp<Boolean> enrichProp = new AttrConfigProp<>();
        enrichProp.setCustomValue(selectThisAttr);
        Map<String, AttrConfigProp<?>> property = new HashMap<>();
        property.put(propertyName, enrichProp);
        config.setAttrProps(property);
        attrConfigs.add(config);
    }

    private BusinessEntity getEntity(Category category, String attrName) {
        if (Category.PRODUCT_SPEND.equals(category)) {
            if (ApsGeneratorUtils.isApsAttr(attrName)) {
                return BusinessEntity.AnalyticPurchaseState;
            } else {
                return BusinessEntity.PurchaseHistory;
            }
        } else {
            return CategoryUtils.getEntity(category).get(0);
        }
    }

    @Override
    public AttributeSet cloneAttributeSet(String name, AttributeSet attributeSet) {
        validateAttributeSet(true, attributeSet);
        return attributeSetEntityMgr.createAttributeSet(name, attributeSet);
    }

    @Override
    public AttributeSetResponse createAttributeSet(AttributeSet attributeSet) {
        validateAttributeSet(true, attributeSet);
        AttributeSetResponse attributeSetResponse = new AttributeSetResponse();
        AttrConfigRequest attrConfigRequest = validateRequest(null, attributeSet);
        attributeSetResponse.setAttrConfigRequest(attrConfigRequest);
        if (attrConfigRequest.hasWarning() || attrConfigRequest.hasError()) {
            log.warn("current attribute configs has warnings or errors:" + JsonUtils.serialize(attrConfigRequest.getDetails()));
            return attributeSetResponse;
        }
        AttributeSet attributeSetToReturn = attributeSetEntityMgr.createAttributeSet(attributeSet);
        attributeSetResponse.setAttributeSet(attributeSetToReturn);
        return attributeSetResponse;
    }

    @Override
    public AttributeSetResponse updateAttributeSet(AttributeSet attributeSet) {
        validateAttributeSet(false, attributeSet);
        AttributeSetResponse attributeSetResponse = new AttributeSetResponse();
        AttributeSet existingAttributeSet = attributeSetEntityMgr.findByName(attributeSet.getName());
        if (existingAttributeSet == null) {
            throw new RuntimeException(String.format("Attribute set %s does not exist.", attributeSet.getName()));
        }
        AttrConfigRequest attrConfigRequest = validateRequest(existingAttributeSet, attributeSet);
        attributeSetResponse.setAttrConfigRequest(attrConfigRequest);
        if (attrConfigRequest.hasWarning() || attrConfigRequest.hasError()) {
            log.warn("current attribute configs has warnings or errors:" + JsonUtils.serialize(attrConfigRequest.getDetails()));
            return attributeSetResponse;
        }
        AttributeSet attributeSetToReturn = attributeSetEntityMgr.updateAttributeSet(attributeSet);
        attributeSetResponse.setAttributeSet(attributeSetToReturn);
        return attributeSetResponse;
    }

    @Override
    public void deleteAttributeSetByName(String name) {
        if (AttributeUtils.isDefaultAttributeSet(name)) {
            throw new LedpException(LedpCode.LEDP_40087, new String[]{AttributeUtils.DEFAULT_ATTRIBUTE_SET_DISPLAY_NAME});
        }
        attributeSetEntityMgr.deleteByName(name);
    }

}
