package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.impl.HGData;

@Component
public class HGDataRefreshServiceImplTestNG extends RefreshBulkServiceImplTestNGBase {

    @Inject
    HGDataRefreshService refreshService;

    @Inject
    BulkArchiveServiceImplTestNGBase hgDataRawArchiveServiceImplDeploymentTestNG;

    @Inject
    HGData source;

    @Inject
    RefreshProgressEntityMgr progressEntityMgr;

    @Override
    RefreshService getRefreshService() {
        return refreshService;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    DerivedSource getSource() { return source; }

    @Override
    BulkArchiveServiceImplTestNGBase getBaseSourceTestBean() {
        return hgDataRawArchiveServiceImplDeploymentTestNG;
    }

}
