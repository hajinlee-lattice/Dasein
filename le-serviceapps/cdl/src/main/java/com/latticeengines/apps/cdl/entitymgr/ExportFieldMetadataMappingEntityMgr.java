package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;

public interface ExportFieldMetadataMappingEntityMgr {

    List<ExportFieldMetadataMapping> createAll(List<ExportFieldMetadataMapping> exportFieldMappings);

    List<ExportFieldMetadataMapping> findByOrgId(String orgId);

    List<ExportFieldMetadataMapping> update(List<ExportFieldMetadataMapping> exportFieldMappings);
}
