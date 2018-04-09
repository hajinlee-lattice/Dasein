package com.latticeengines.apps.cdl.service.impl;

import java.util.List;

import javax.inject.Inject;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.stereotype.Service;

import com.latticeengines.apps.cdl.service.DataCollectionService;
import com.latticeengines.apps.cdl.service.ServingStoreService;
import com.latticeengines.apps.core.entitymgr.AttrConfigEntityMgr;
import com.latticeengines.apps.core.service.AttrConfigService;
import com.latticeengines.apps.core.service.impl.AbstractAttrConfigService;
import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.serviceapps.core.AttrConfig;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.monitor.exposed.metrics.PerformanceTimer;

@Service("cdlAttrConfigService")
public class CDLAttrConfigServiceImpl extends AbstractAttrConfigService implements AttrConfigService {

    @Inject
    private DataCollectionService dataCollectionService;

    @Inject
    private ServingStoreService servingStoreService;

    @Inject
    private AttrConfigEntityMgr attrConfigEntityMgr;

    protected List<ColumnMetadata> getSystemMetadata(BusinessEntity entity) {
        String tenantId = MultiTenantContext.getTenantId();
        DataCollection.Version version = dataCollectionService.getActiveVersion(tenantId);
        return servingStoreService.getSystemMetadata(entity, version) //
                .sequential().collectList().block();
    }

    protected List<ColumnMetadata> getSystemMetadata(Category category) {
        BusinessEntity entity = CategoryUtils.getEntity(category);
        String tenantId = MultiTenantContext.getTenantId();
        DataCollection.Version version = dataCollectionService.getActiveVersion(tenantId);
        return servingStoreService.getSystemMetadata(entity, version) //
                .filter(cm -> category.equals(cm.getCategory())) //
                .sequential().collectList().block();
    }

    @Override
    public List<AttrConfig> getRenderedList(BusinessEntity entity) {
        String tenantId = MultiTenantContext.getTenantId();
        List<AttrConfig> renderedList;
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntity(tenantId, entity);
            List<ColumnMetadata> columns = getSystemMetadata(entity);
            renderedList = render(columns, customConfig);
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs", count);
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

    @Override
    public List<AttrConfig> getRenderedList(Category category) {
        List<AttrConfig> renderedList;
        String tenantId = MultiTenantContext.getTenantId();
        BusinessEntity entity = CategoryUtils.getEntity(category);
        try (PerformanceTimer timer = new PerformanceTimer()) {
            List<AttrConfig> customConfig = attrConfigEntityMgr.findAllForEntity(tenantId, entity);
            List<ColumnMetadata> columns = getSystemMetadata(category);
            renderedList = render(columns, customConfig);
            int count = CollectionUtils.isNotEmpty(renderedList) ? renderedList.size() : 0;
            String msg = String.format("Rendered %d attr configs", count);
            timer.setTimerMessage(msg);
        }
        return renderedList;
    }

}
