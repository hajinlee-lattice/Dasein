package com.latticeengines.propdata.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.source.MostRecentSource;
import com.latticeengines.propdata.core.source.impl.FeatureMostRecent;

@Component
public class FeatureRefreshServiceImplTestNG extends MostRecentServiceImplTestNGBase {

    @Autowired
    FeatureRefreshService refreshService;

    @Autowired
    FeatureArchiveServiceImplTestNG archiveServiceImplDeploymentTestNG;

    @Autowired
    FeatureMostRecent source;

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
    protected Integer getExpectedRows() { return 82; }
}
