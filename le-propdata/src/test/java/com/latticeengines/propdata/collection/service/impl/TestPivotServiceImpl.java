package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.DataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.strategy.impl.PivotStrategyImpl;
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

    @Autowired
    @Qualifier(value = "testPivotedSource")
    PivotedSource source;

    @Override
    public PivotedSource getSource() { return source; }

    @Override
    PivotProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    ArchiveProgressEntityMgr getBaseSourceArchiveProgressEntityMgr() { return archiveProgressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    DataFlowBuilder.FieldList getGroupByFields() { return null; }

    @Override
    PivotStrategyImpl getPivotStrategy() { return null; }

}
