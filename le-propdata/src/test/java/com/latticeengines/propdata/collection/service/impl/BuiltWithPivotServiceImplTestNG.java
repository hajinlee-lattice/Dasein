package com.latticeengines.propdata.collection.service.impl;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.core.source.PivotedSource;
import com.latticeengines.propdata.core.source.impl.BuiltWithPivoted;

@Component
public class BuiltWithPivotServiceImplTestNG extends PivotServiceImplTestNGBase {

    @Autowired
    BuiltWithPivotService pivotService;

    @Autowired
    BuiltWithPivoted source;

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
    Integer getExpectedRows() { return 48; }

}
