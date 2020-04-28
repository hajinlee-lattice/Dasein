package com.latticeengines.apps.cdl.service.impl;

import java.util.ArrayList;
import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.entitymgr.AttributeSetEntityMgr;
import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.common.exposed.timer.PerformanceTimer;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.metadata.AttributeSet;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.util.CategoryUtils;

@Service("cdlAttrConfigService")
public class CDLAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    @Inject
    private AttributeSetEntityMgr attributeSetEntityMgr;

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
    public List<AttributeSet> getAttributeSets() {
        return attributeSetEntityMgr.findAll();
    }

    private void validateAttributeSet(AttributeSet attributeSet) {
        if (StringUtils.isBlank(attributeSet.getDisplayName())) {
            throw new LedpException(LedpCode.LEDP_18244);
        }
        AttributeSet existingAttributeSet = attributeSetEntityMgr.findByDisplayName(attributeSet.getDisplayName());
        if (existingAttributeSet != null) {
            if (StringUtils.isEmpty(attributeSet.getName())) {
                throw new LedpException(LedpCode.LEDP_18243, new String[]{attributeSet.getDisplayName()});
            } else {
                if (!existingAttributeSet.getName().equals(attributeSet.getName())) {
                    throw new LedpException(LedpCode.LEDP_18243, new String[]{attributeSet.getDisplayName()});
                }
            }
        }
    }

    @Override
    public AttributeSet createOrUpdateAttributeSet(AttributeSet attributeSet) {
        validateAttributeSet(attributeSet);
        if (StringUtils.isNotEmpty(attributeSet.getName())) {
            return attributeSetEntityMgr.updateAttributeSet(attributeSet);
        } else {
            return attributeSetEntityMgr.createAttributeSet(attributeSet);
        }
    }

    @Override
    public void deleteAttributeSetByName(String name) {
        attributeSetEntityMgr.deleteByName(name);
    }

}
