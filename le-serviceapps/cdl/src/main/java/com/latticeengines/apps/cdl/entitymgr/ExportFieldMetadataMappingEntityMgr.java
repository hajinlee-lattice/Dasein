package com.latticeengines.apps.cdl.entitymgr;

import java.util.List;

import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface ExportFieldMetadataMappingEntityMgr {

    List<ExportFieldMetadataMapping> createAll(List<ExportFieldMetadataMapping> exportFieldMappings);

    List<ExportFieldMetadataMapping> findByOrgId(String orgId, Long tenantPid);

    List<ExportFieldMetadataMapping> update(LookupIdMap lookupIdMap);
}
