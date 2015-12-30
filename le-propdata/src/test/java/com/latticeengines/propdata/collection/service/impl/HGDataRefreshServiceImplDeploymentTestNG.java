package com.latticeengines.propdata.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.collection.source.ServingSource;
import com.latticeengines.propdata.collection.source.impl.HGData;

@Component
public class HGDataRefreshServiceImplDeploymentTestNG extends RefreshBulkServiceImplDeploymentTestNGBase {

    @Autowired
    HGDataRefreshService refreshService;

    @Autowired
    HGDataRawArchiveServiceImplDeploymentTestNG hgDataRawArchiveServiceImplDeploymentTestNG;

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
    BulkArchiveServiceImplDeploymentTestNGBase getBaseSourceTestBean() {
        return hgDataRawArchiveServiceImplDeploymentTestNG;
    }

}
