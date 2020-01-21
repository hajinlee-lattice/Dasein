package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectedArchiveService;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.impl.OrbIntelligence;

@Component("orbIntelligenceArchiveService")
public class OrbIntelligenceArchiveService extends AbstractCollectionArchiveService implements CollectedArchiveService {

    @Inject
    private ArchiveProgressEntityMgr progressEntityMgr;

    @Inject
    private OrbIntelligence source;

    @Override
    public String getBeanName() {
        return "orbIntelligenceArchiveService";
    }

    @Override
    public CollectedSource getSource() { return source; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

}
