package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.impl.TestPivotedSource;

@Component("testPivotService")
public class TestPivotService extends AbstractPivotService implements PivotService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Autowired
    TestPivotedSource source;

    @Override
    public PivotedSource getSource() { return source; }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    DataFlowBuilder.FieldList getGroupByFields() { return null; }

    @Override
    PivotStrategyImpl getPivotStrategy() { return null; }

    @Override
    public String createStageTableSql() { return ""; }

}
