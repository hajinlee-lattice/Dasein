package com.latticeengines.admin.document.repository.writer;

import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.transaction.annotation.Transactional;

import com.latticeengines.documentdb.entity.TenantConfigEntity;
import com.latticeengines.documentdb.repository.AdminDocumentRepository;
import com.latticeengines.domain.exposed.security.TenantStatus;

public interface TenantConfigEntityWriterRepository extends AdminDocumentRepository<TenantConfigEntity> {

    TenantConfigEntity findByContractIdAndTenantId(String contract, String tenantId);

    @Transactional
    @Modifying
    void removeByContractIdAndTenantId(String contractId, String tenantId);

    @Transactional
    @Modifying(clearAutomatically = true)
    @Query("UPDATE TenantConfigEntity t SET t.status = :status WHERE t.pid = :pid")
    void updateTenantStatus(@Param("pid") Long pid, @Param("status") TenantStatus status);
}
