package com.latticeengines.admin.entitymgr.impl;

import java.util.List;

import javax.inject.Inject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.admin.document.repository.reader.TenantConfigEntityReaderRepository;
import com.latticeengines.admin.document.repository.writer.TenantConfigEntityWriterRepository;
import com.latticeengines.admin.entitymgr.TenantConfigEntityMgr;
import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.documentdb.entity.TenantConfigEntity;
import com.latticeengines.documentdb.entitymgr.impl.AdminDocumentEntityMgrImpl;
import com.latticeengines.domain.exposed.admin.SpaceConfiguration;
import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.camille.featureflags.FeatureFlagValueMap;
import com.latticeengines.domain.exposed.camille.lifecycle.ContractProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.CustomerSpaceProperties;
import com.latticeengines.domain.exposed.camille.lifecycle.TenantProperties;
import com.latticeengines.domain.exposed.security.TenantStatus;

@Component("TenantConfigMgr")
public class TenantConfigEntityMgrImpl extends AdminDocumentEntityMgrImpl<TenantConfigEntity>
        implements TenantConfigEntityMgr {

    private static final Logger log = LoggerFactory.getLogger(TenantConfigEntityMgrImpl.class);

    @Inject
    private TenantConfigEntityWriterRepository tenantConfigEntityWriterRepository;

    @Inject
    private TenantConfigEntityReaderRepository tenantConfigEntityReaderRepository;

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public boolean createTenant(String contractId, String tenantId, ContractProperties contractProperties,
            TenantProperties tenantProperties,
            CustomerSpaceProperties customerSpaceProperties, FeatureFlagValueMap featureFlags,
            SpaceConfiguration spaceConfiguration) {

        TenantConfigEntity entity = new TenantConfigEntity();
        entity.setContractId(contractId);
        entity.setTenantId(tenantId);
        entity.setStatus(TenantStatus.INITING);
        entity.setDefaultSpace(CustomerSpace.BACKWARDS_COMPATIBLE_SPACE_ID);
        entity.setContractPropertities(contractProperties);
        entity.setTenantProperties(tenantProperties);
        entity.setSpaceProperties(customerSpaceProperties);
        entity.setSpaceConfiguration(spaceConfiguration);
        entity.setFeatureFlags(featureFlags);
        tenantConfigEntityWriterRepository.save(entity);
        return true;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<TenantConfigEntity> getTenants() {
        return tenantConfigEntityWriterRepository.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TenantConfigEntity getTenant(String contractId, String tenantId) {
        return tenantConfigEntityWriterRepository.findByContractIdAndTenantId(contractId, tenantId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public List<TenantConfigEntity> getTenantsInReader() {
        return tenantConfigEntityReaderRepository.findAll();
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, readOnly = true)
    public TenantConfigEntity getTenantInReader(String contractId, String tenantId) {
        return tenantConfigEntityReaderRepository.findByContractIdAndTenantId(contractId, tenantId);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public boolean deleteTenant(String contractId, String tenantId) {
        tenantConfigEntityWriterRepository.removeByContractIdAndTenantId(contractId, tenantId);
        return true;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public void updateTenantStatus(Long pid, TenantStatus status) {
        tenantConfigEntityWriterRepository.updateTenantStatus(pid, status);
    }

    @Override
    public BaseJpaRepository<TenantConfigEntity, Long> getRepository() {
        return tenantConfigEntityWriterRepository;
    }

}
