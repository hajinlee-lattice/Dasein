package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.source.CollectionSource;

@Component("builtWithArchiveService")
public class BuiltWithArchiveServiceImpl extends AbstractArchiveService implements ArchiveService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Override
    public CollectionSource getSource() { return CollectionSource.BUILTWITH; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    String getSourceTableName() { return "BuiltWith"; }

    @Override
    String getMergeDataFlowQualifier() { return "builtWithMergeRawDataFlowBuilder"; }

    @Override
    String getSrcTableSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String getSrcTableTimestampColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String createIndexForStageTableSql() {
        return "CREATE CLUSTERED INDEX IX_URLFeature ON [BuiltWith_MostRecent_stage] ([Domain], [Technology_Name])";
    }
}
