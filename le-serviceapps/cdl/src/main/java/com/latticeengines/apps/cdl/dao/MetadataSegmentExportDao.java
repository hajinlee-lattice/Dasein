package com.latticeengines.apps.cdl.dao;

import com.latticeengines.db.exposed.dao.BaseDao;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;

public interface MetadataSegmentExportDao extends BaseDao<MetadataSegmentExport> {

    MetadataSegmentExport findByExportId(String exportId);

    void deleteByExportId(String exportId);
}
