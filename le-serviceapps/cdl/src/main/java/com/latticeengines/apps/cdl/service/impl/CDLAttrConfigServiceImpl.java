package com.latticeengines.apps.cdl.service.impl;

import static java.util.stream.Collectors.mapping;
import static java.util.stream.Collectors.toSet;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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
import com.latticeengines.common.exposed.util.ThreadPoolUtils;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.propdata.manage.ColumnSelection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.security.Tenant;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
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

    private static final String DEFAULT_ATTRIBUTE_SET = "Default Group";

    @Override
    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        String tenantId = MultiTenantContext.getShortTenantId();
        DataCollection.Version version = dataCollectionService.getActiveVersion(tenantId);
        return servingStoreService.getSystemMetadata(entity, version) //
                .sequential().collectList().block();
    }

    private Map<String, Set<String>> getCustomAttributesMap() {
        final Tenant tenant = MultiTenantContext.getTenant();
        Set<BusinessEntity> entitySet = new HashSet<>(BusinessEntity.EXPORT_ACCOUNT_ENTITIES);
        entitySet.add(BusinessEntity.Contact);
        List<ColumnSelection.Predefined> groups = Collections.singletonList(ColumnSelection.Predefined.Enrichment);
        List<Runnable> runnables = new ArrayList<>();
        Map<String, Set<String>> attributesMap = new ConcurrentHashMap<>();
        entitySet.forEach(businessEntity -> {
            Runnable runnable = () -> {
                MultiTenantContext.setTenant(tenant);
                Map<String, Set<String>> columnMetadataMap = getDecoratedMetadata(businessEntity, groups).stream()
                        .collect(Collectors.groupingBy(columnMetadata -> columnMetadata.getCategory().name(), mapping(ColumnMetadata::getAttrName, toSet())));
                attributesMap.putAll(columnMetadataMap);
            };
            runnables.add(runnable);
        });
        ThreadPoolUtils.runInParallel("custom-metadata", runnables);
        return attributesMap;
    }

    private List<ColumnMetadata> getDecoratedMetadata(BusinessEntity entity, List<ColumnSelection.Predefined> groups) {
        String tenantId = MultiTenantContext.getShortTenantId();
        DataCollection.Version version = dataCollectionService.getActiveVersion(tenantId);
        return servingStoreService.getDecoratedMetadata(tenantId, entity, version, groups).collectList().block();
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
    public List<AttributeSet> getAttributeSets() {
        List<AttributeSet> attributeSets = new ArrayList<>();
        attributeSets.add(getDefaultAttributeSet());
        List<AttributeSet> returnedAttributeSets = attributeSetEntityMgr.findAll();
        if (CollectionUtils.isNotEmpty(returnedAttributeSets)) {
            attributeSets.addAll(returnedAttributeSets);
        }
        return attributeSets;
    }

    private AttributeSet getDefaultAttributeSet() {
        AttributeSet attributeSet = new AttributeSet();
        attributeSet.setDisplayName(DEFAULT_ATTRIBUTE_SET);
        return attributeSet;
    }

    @Override
    public AttributeSet createAttributeSet(String name, AttributeSet attributeSet) {
        if (StringUtils.isEmpty(name)) {
            // get exist attribute map from custom serving store
            Map<String, Set<String>> existingAttributesMap = getCustomAttributesMap();
            return attributeSetEntityMgr.createAttributeSet(existingAttributesMap, attributeSet);
        } else {
            return attributeSetEntityMgr.createAttributeSet(name, attributeSet);
        }
    }

    @Override
    public AttributeSet createAttributeSet(AttributeSet attributeSet) {
        return attributeSetEntityMgr.createAttributeSet(attributeSet);
    }

    @Override
    public AttributeSet updateAttributeSet(AttributeSet attributeSet) {
        return attributeSetEntityMgr.updateAttributeSet(attributeSet);
    }

    @Override
    public void deleteAttributeSetByName(String name) {
        attributeSetEntityMgr.deleteByName(name);
    }

}
