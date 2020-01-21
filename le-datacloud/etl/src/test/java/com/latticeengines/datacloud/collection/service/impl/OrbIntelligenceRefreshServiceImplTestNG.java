package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.impl.OrbIntelligenceMostRecent;

@Component
public class OrbIntelligenceRefreshServiceImplTestNG extends MostRecentServiceImplTestNGBase {

    @Inject
    OrbIntelligenceRefreshService refreshService;

    @Inject
    OrbIntelligenceArchiveServiceImplTestNG archiveServiceImplDeploymentTestNG;

    @Inject
    OrbIntelligenceMostRecent source;

    @Inject
    RefreshProgressEntityMgr progressEntityMgr;

    @Override
    RefreshService getRefreshService() {
        return refreshService;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    MostRecentSource getSource() { return source; }

    @Override
    CollectionArchiveServiceImplTestNGBase getBaseSourceTestBean() {
        return archiveServiceImplDeploymentTestNG;
    }

    @Override
    protected Integer getExpectedRows() { return 400; }
}
