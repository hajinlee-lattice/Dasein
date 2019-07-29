package com.latticeengines.apps.cdl.entitymgr;

import com.latticeengines.db.exposed.entitymgr.BaseEntityMgrRepository;
import com.latticeengines.domain.exposed.cdl.AtlasExport;
import com.latticeengines.domain.exposed.pls.AtlasExportType;

public interface AtlasExportEntityMgr extends BaseEntityMgrRepository<AtlasExport, Long> {

    AtlasExport findByUuid(String uuid);

    AtlasExport createAtlasExport(AtlasExportType exportType);

    AtlasExport createAtlasExport(AtlasExport atlasExport);
}
