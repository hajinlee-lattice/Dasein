package com.latticeengines.datacloud.collection.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectedArchiveService;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.impl.OrbIntelligence;

@Component("orbIntelligenceArchiveService")
public class OrbIntelligenceArchiveService extends AbstractCollectionArchiveService implements CollectedArchiveService {

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Autowired
    OrbIntelligence source;

    @Override
    public String getBeanName() {
        return "orbIntelligenceArchiveService";
    }

    @Override
    public CollectedSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Logger getLogger() { return log; }
}
