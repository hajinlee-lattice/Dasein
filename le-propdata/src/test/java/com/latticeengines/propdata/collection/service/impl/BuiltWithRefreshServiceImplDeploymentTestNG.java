package com.latticeengines.propdata.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.collection.source.MostRecentSource;
import com.latticeengines.propdata.collection.source.impl.BuiltWithMostRecent;

@Component
public class BuiltWithRefreshServiceImplDeploymentTestNG extends MostRecentServiceImplDeploymentTestNGBase {

    @Autowired
    BuiltWithRefreshService refreshService;

    @Autowired
    BuiltWithArchiveServiceImplDeploymentTestNG archiveServiceImplDeploymentTestNG;

    @Autowired
    BuiltWithMostRecent source;

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
    CollectionArchiveServiceImplDeploymentTestNGBase getBaseSourceTestBean() {
        return archiveServiceImplDeploymentTestNG;
    }

}
