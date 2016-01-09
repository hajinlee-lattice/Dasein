package com.latticeengines.propdata.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.BulkArchiveService;
import com.latticeengines.propdata.collection.source.BulkSource;
import com.latticeengines.propdata.collection.source.impl.HGDataRaw;

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
