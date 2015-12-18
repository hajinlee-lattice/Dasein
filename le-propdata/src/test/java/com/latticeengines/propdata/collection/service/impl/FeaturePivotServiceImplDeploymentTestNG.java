package com.latticeengines.propdata.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.PivotProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;

@Component
public class FeaturePivotServiceImplDeploymentTestNG extends PivotServiceImplDeploymentTestNGBase {

    @Autowired
    @Qualifier(value = "featurePivotService")
    PivotService pivotService;

    @Autowired
    PivotProgressEntityMgr progressEntityMgr;

    @Override
    PivotService getPivotService() {
        return pivotService;
    }

    @Override
    PivotProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    PivotedSource getSource() { return PivotedSource.FEATURE_PIVOTED; }

}
