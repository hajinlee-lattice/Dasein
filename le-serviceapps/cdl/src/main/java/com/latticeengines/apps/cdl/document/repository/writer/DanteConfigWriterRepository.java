package com.latticeengines.apps.cdl.document.repository.writer;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.documentdb.entity.DanteConfigEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;
import com.latticeengines.domain.exposed.dante.DanteConfigurationDocument;

public interface DanteConfigWriterRepository extends MultiTenantDocumentRepository<DanteConfigEntity> {

    long countByTenantId(String tenantId);

    DanteConfigEntity findByTenantId(String tenantId);
}
