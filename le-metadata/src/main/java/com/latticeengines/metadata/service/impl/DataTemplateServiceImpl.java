package com.latticeengines.metadata.service.impl;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import com.latticeengines.db.exposed.util.MultiTenantContext;
import com.latticeengines.domain.exposed.metadata.datastore.DataTemplate;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;
import com.latticeengines.metadata.entitymgr.DataTemplateEntityMgr;
import com.latticeengines.metadata.service.DataTemplateService;

@Service("dataTemplateService")
public class DataTemplateServiceImpl implements DataTemplateService {

    private static final Logger log = LoggerFactory.getLogger(DataTemplateServiceImpl.class);

    @Inject
    private DataTemplateEntityMgr entityMgr;

    @Override
    public DataUnit createDataTemplate(DataTemplate dataTemplate, DataUnit dataUnit) {
        return null;
    }

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
}
