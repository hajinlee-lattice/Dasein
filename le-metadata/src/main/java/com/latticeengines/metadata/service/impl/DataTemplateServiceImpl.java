package com.latticeengines.metadata.service.impl;

import java.util.Map;
import java.util.stream.Collectors;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.Category;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.query.BusinessEntity;
import com.latticeengines.domain.exposed.util.CategoryUtils;
import com.latticeengines.metadata.entitymgr.DataTemplateEntityMgr;
import com.latticeengines.metadata.service.DataTemplateService;

@Service("dataTemplateService")
public class DataTemplateServiceImpl implements DataTemplateService {

    private static final Logger log = LoggerFactory.getLogger(DataTemplateServiceImpl.class);

    @Inject
    private DataTemplateEntityMgr entityMgr;

    @Override
    public String create(DataTemplate dataTemplate) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.create(tenantId, dataTemplate);
    }

    @Override
    public DataTemplate findByUuid(String uuid) {
        String tenantId = MultiTenantContext.getShortTenantId();
        return entityMgr.findByUuid(tenantId, uuid);
    }

    @Override
    public void updateByUuid(String uuid, DataTemplate dataTemplate) {
        String tenantId = MultiTenantContext.getShortTenantId();
        entityMgr.updateByUuid(tenantId, uuid, dataTemplate);
    }

    @Override
    public void deleteByUuid(String uuid) {
        String tenantId = MultiTenantContext.getShortTenantId();
        entityMgr.deleteByUuid(tenantId, uuid);
    }

    @Override
    public Map<String, ColumnMetadata> getTemplateMetadata(String templateId, BusinessEntity entity) {
        DataTemplate dataTemplate = findByUuid(templateId);
        Category category = CategoryUtils.getEntityCategory(entity);
        return dataTemplate.getMasterSchema().getFields().stream().collect(Collectors.toMap(columnField -> columnField.getAttrName(), columnField -> {
            ColumnMetadata columnMetadata = columnField.toColumnMetadata();
            columnMetadata.setEntity(entity);
            columnMetadata.setCategory(category);
            return columnMetadata;
        }));
    }
}
