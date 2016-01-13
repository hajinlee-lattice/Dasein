package com.latticeengines.propdata.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.source.ServingSource;
import com.latticeengines.propdata.core.source.impl.HGData;

@Component
public class HGDataRefreshServiceImplTestNG extends RefreshBulkServiceImplTestNGBase {

    @Autowired
    HGDataRefreshService refreshService;

    @Autowired
    BulkArchiveServiceImplTestNGBase hgDataRawArchiveServiceImplDeploymentTestNG;

    @Autowired
    HGData source;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Override
    RefreshService getRefreshService() {
        return refreshService;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    ServingSource getSource() { return source; }

    @Override
    BulkArchiveServiceImplTestNGBase getBaseSourceTestBean() {
        return hgDataRawArchiveServiceImplDeploymentTestNG;
    }

}
