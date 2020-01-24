package com.latticeengines.datacloud.collection.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.PivotService;
import com.latticeengines.datacloud.core.source.PivotedSource;
import com.latticeengines.datacloud.core.source.impl.FeaturePivoted;

@Component
public class FeaturePivotServiceImplTestNG extends PivotServiceImplTestNGBase {

    @Inject
    FeaturePivotService pivotService;

    @Inject
    FeaturePivoted source;

    @Inject
    RefreshProgressEntityMgr progressEntityMgr;

    @Override
    PivotService getPivotService() {
        return pivotService;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() {
        return progressEntityMgr;
    }

    @Override
    PivotedSource getSource() {
        return source;
    }

    @Override
    Integer getExpectedRows() {
        return 3;
    }

}
