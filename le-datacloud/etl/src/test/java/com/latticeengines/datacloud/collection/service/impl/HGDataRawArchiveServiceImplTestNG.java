package com.latticeengines.datacloud.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.BulkArchiveService;
import com.latticeengines.datacloud.core.source.BulkSource;
import com.latticeengines.datacloud.core.source.impl.HGDataRaw;

@Component
public class HGDataRawArchiveServiceImplTestNG extends BulkArchiveServiceImplTestNGBase {

    @Autowired
    HGDataRawArchiveService archiveService;

    @Autowired
    HGDataRaw source;

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Override
    BulkArchiveService getArchiveService() {
        return archiveService;
    }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    BulkSource getSource() { return source; }

}
