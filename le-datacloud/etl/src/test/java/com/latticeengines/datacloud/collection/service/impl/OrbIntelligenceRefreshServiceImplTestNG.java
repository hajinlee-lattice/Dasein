package com.latticeengines.datacloud.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.MostRecentSource;
import com.latticeengines.datacloud.core.source.impl.OrbIntelligenceMostRecent;


@Component
public class OrbIntelligenceRefreshServiceImplTestNG extends MostRecentServiceImplTestNGBase {

    @Autowired
    OrbIntelligenceRefreshService refreshService;

    @Autowired
    OrbIntelligenceArchiveServiceImplTestNG archiveServiceImplDeploymentTestNG;

    @Autowired
    OrbIntelligenceMostRecent source;

    @Autowired
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
