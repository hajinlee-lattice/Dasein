package com.latticeengines.admin.entitymgr.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.admin.document.repository.writer.ServiceConfigEntityWriterRepository;
import com.latticeengines.admin.entitymgr.ServiceConfigEntityMgr;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.ServiceConfigEntity;
import com.latticeengines.documentdb.entity.TenantConfigEntity;
import com.latticeengines.documentdb.entitymgr.impl.AdminDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.admin.SerializableDocumentDirectory;
import com.latticeengines.domain.exposed.camille.bootstrap.BootstrapState;

@Component("serviceConfigEntityMgr")
public class ServiceConfigEntityMgrImpl extends AdminDocumentEntityMgrImpl<ServiceConfigEntity>
        implements ServiceConfigEntityMgr {

    @Inject
    private ServiceConfigEntityWriterRepository serviceConfigEntityWriterRepository;

    @Override
    public BaseJpaRepository<ServiceConfigEntity, Long> getRepository() {
        return serviceConfigEntityWriterRepository;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public ServiceConfigEntity createServiceForTenant(TenantConfigEntity tenantConfig, String serviceName,
            SerializableDocumentDirectory serviceConfig,
            BootstrapState state) {
        ServiceConfigEntity entity = new ServiceConfigEntity();
        entity.setTenantConfigEntity(tenantConfig);
        entity.setServiceConfig(serviceConfig);
        entity.setServiceName(serviceName);
        entity.setServiceConfig(serviceConfig);
        entity.setState(state);
        return serviceConfigEntityWriterRepository.save(entity);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, readOnly = true)
    public ServiceConfigEntity getTenantService(TenantConfigEntity entity, String serviceName) {
        return serviceConfigEntityWriterRepository.findByTenantConfigEntityAndServiceName(entity, serviceName);
    }

}
