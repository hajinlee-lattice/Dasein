package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.source.CollectionSource;

@Component("testArchiveService")
public class TestArchiveServiceImpl extends AbstractArchiveService implements ArchiveService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Override
    public CollectionSource getSource() { return CollectionSource.TEST_COLLECTION; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    String getSourceTableName() { return "Nope"; }

    @Override
    String getMergeDataFlowQualifier() { return "nope"; }

    @Override
    String getSrcTableSplitColumn() { return "nope"; }

    @Override
    String getSrcTableTimestampColumn() { return "nope"; }

    @Override
    String createIndexForStageTableSql() { return "nope"; }
}
