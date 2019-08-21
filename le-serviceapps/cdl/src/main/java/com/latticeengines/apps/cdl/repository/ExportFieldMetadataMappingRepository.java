package com.latticeengines.apps.cdl.repository;

import java.util.List;

import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import com.latticeengines.db.exposed.repository.BaseJpaRepository;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;

public interface ExportFieldMetadataMappingRepository extends BaseJpaRepository<ExportFieldMetadataMapping, Long> {

    @Query(name = ExportFieldMetadataMapping.NQ_FIND_FIELD_MAPPING_BY_LOOKUPMAP_ORG_ID)
    List<ExportFieldMetadataMapping> findByOrgId(@Param("orgId") String orgId, @Param("tenantPid") Long tenantPid);

    @Query(name = ExportFieldMetadataMapping.NQ_FIND_ALL_FIELD_MAPPINGS)
    List<ExportFieldMetadataMapping> findAllFieldMappings();
}
