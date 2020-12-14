package com.latticeengines.metadata.repository.document;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.NoRepositoryBean;
import org.springframework.data.repository.query.Param;

import com.latticeengines.documentdb.entity.DataUnitEntity;
import com.latticeengines.documentdb.repository.MultiTenantDocumentRepository;
import com.latticeengines.domain.exposed.metadata.datastore.DataUnit;

@NoRepositoryBean
public interface DataUnitRepository extends MultiTenantDocumentRepository<DataUnitEntity> {

    List<DataUnitEntity> findByName(String name);

    List<DataUnitEntity> findByTenantIdAndStorageType(String tenantId, DataUnit.StorageType storageType);

    List<DataUnitEntity> findByTenantIdAndName(String tenantId, String name);

    List<DataUnitEntity> findByTenantId(String tenantId);

    DataUnitEntity findByTenantIdAndNameAndStorageType(String tenantId, String name, DataUnit.StorageType storageType);

    List<DataUnitEntity> findByTenantIdAndDataTemplateId(String tenantId, String dataTemplateId);

    List<DataUnitEntity> findByTenantIdAndDataTemplateIdOrderByCreatedDate(String tenantId, String dataTemplateId);

    @Query("SELECT m FROM DataUnitEntity m WHERE m.retentionPolicy != null AND m.retentionPolicy != :noExpirePolicy")
    List<DataUnitEntity> findAllDataUnitEntitiesWithExpiredRetentionPolicy(@Param("noExpirePolicy") String noExpirePolicy);
}
