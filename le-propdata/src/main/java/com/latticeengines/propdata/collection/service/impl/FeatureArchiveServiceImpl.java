package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.FeatureArchiveService;
import com.latticeengines.propdata.collection.source.CollectionSource;

@Component("featureArchiveService")
public class FeatureArchiveServiceImpl extends AbstractArchiveService implements FeatureArchiveService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr progressEntityMgr;

    @Override
    public CollectionSource getSource() { return CollectionSource.FEATURE; }

    @Override
    ArchiveProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    String getSourceTableName() { return "Feature"; }

    @Override
    String getMergeDataFlowQualifier() { return "featureMergeRawSnapshotDataFlowBuilder"; }

    @Override
    String getSrcTableSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String getDestTableSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String getSrcTableTimestampColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String createIndexForStageTableSql() {
        return "CREATE CLUSTERED INDEX IX_URLFeature ON [Feature_MostRecent_stage] ([URL], [Feature])";
    }
}
