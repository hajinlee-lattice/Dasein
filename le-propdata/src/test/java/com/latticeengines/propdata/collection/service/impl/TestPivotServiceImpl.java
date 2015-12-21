package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.PivotProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;

@Component("testPivotService")
public class TestPivotServiceImpl extends AbstractPivotService implements PivotService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    PivotProgressEntityMgr progressEntityMgr;

    @Override
    public PivotedSource getSource() { return PivotedSource.TEST_PIVOTED; }

    @Override
    PivotProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    ArchiveProgressEntityMgr getBaseSourceArchiveProgressEntityMgr() { return archiveProgressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    String getPivotDataFlowQualifier() { return ""; }

    @Override
    String createIndexForStageTableSql() { return ""; }
}
