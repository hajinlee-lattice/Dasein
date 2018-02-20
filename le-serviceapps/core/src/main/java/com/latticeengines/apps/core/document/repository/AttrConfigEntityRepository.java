package com.latticeengines.apps.core.document.repository;

import java.util.List;

import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.documentdb.entity.AttrConfigEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;
import com.latticeengines.domain.exposed.query.BusinessEntity;

public interface AttrConfigEntityRepository extends MultiTenantDocumentRepository<AttrConfigEntity> {

    long countByTenantIdAndEntity(String tenantId, BusinessEntity entity);

    List<AttrConfigEntity> findByTenantIdAndEntity(String tenantId, BusinessEntity entity, Pageable pageable);

    List<AttrConfigEntity> findByTenantIdAndEntity(String tenantId, BusinessEntity entity);

    @Transactional
    @Modifying
    List<AttrConfigEntity> removeByTenantIdAndEntity(String tenantId, BusinessEntity entity);

}
