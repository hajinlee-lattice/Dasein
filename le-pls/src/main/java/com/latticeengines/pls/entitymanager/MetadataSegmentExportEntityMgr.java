package com.latticeengines.pls.entitymanager;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgr;
import com.latticeengines.domain.exposed.pls.MetadataSegmentExport;

public interface MetadataSegmentExportEntityMgr extends BaseEntityMgr<MetadataSegmentExport> {

    void create(MetadataSegmentExport entity);

    MetadataSegmentExport findByExportId(String exportId);

    void deleteByExportId(String exportId);

}
