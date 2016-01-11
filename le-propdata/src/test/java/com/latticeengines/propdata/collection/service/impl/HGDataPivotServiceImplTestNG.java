package com.latticeengines.propdata.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.impl.HGDataPivoted;

@Component
public class HGDataPivotServiceImplTestNG extends PivotServiceImplTestNGBase {

    @Autowired
    HGDataPivotService pivotService;

    @Autowired
    HGDataPivoted source;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Override
    PivotService getPivotService() {
        return pivotService;
    }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    PivotedSource getSource() { return source; }

    @Override
    Integer getExpectedRows() { return 142; }

}
