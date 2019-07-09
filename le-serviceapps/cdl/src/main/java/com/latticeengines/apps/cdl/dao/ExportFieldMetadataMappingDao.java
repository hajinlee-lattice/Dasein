package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;
import com.latticeengines.domain.exposed.pls.LookupIdMap;

public interface ExportFieldMetadataMappingDao extends BaseDao<ExportFieldMetadataMapping> {

    List<ExportFieldMetadataMapping> updateExportFieldMetadataMappings(LookupIdMap lookupIdMap,
            List<ExportFieldMetadataMapping> exportFieldMetadataMappings);
}
