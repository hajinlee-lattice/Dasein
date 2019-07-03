package com.latticeengines.apps.cdl.dao;

import java.util.List;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.ExportFieldMetadataMapping;

public interface ExportFieldMetadataMappingDao extends BaseDao<ExportFieldMetadataMapping> {

    List<ExportFieldMetadataMapping> updateExportFieldMetadataMappings(
            List<ExportFieldMetadataMapping> exportFieldMetadataMappings);
}
