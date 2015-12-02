package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.FeatureArchiveProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.FeatureArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.FeatureArchiveService;

@Component("featureArchiveService")
public class FeatureArchiveServiceImpl extends AbstractArchiveService<FeatureArchiveProgress>
        implements FeatureArchiveService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    FeatureArchiveProgressEntityMgr progressEntityMgr;

    @Override
    ArchiveProgressEntityMgr<FeatureArchiveProgress> getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    String getSourceTableName() { return "Feature"; }

    @Override
    String getDestTableName() { return "Feature_MostRecent"; }

    @Override
    String getPivotedTableName() { return "Feature_Pivoted_Source"; }

    @Override
    String getMergeDataFlowQualifier() { return "featureMergeRawSnapshotDataFlowBuilder"; }

    @Override
    String getPivotDataFlowQualifier() { return "featurePivotSnapshotDataFlowBuilder"; }

    @Override
    String getSrcTableSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String getDestTableSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String getSrcTableTimestampColumn() { return "LE_Last_Upload_Date"; }

    @Override
    void createIndicesForTables() {
        jdbcTemplateDest.execute("CREATE CLUSTERED INDEX IX_URL ON [Feature_Pivoted_Source] ([URL])");
        jdbcTemplateDest.execute("CREATE CLUSTERED INDEX IX_URLFeature ON [Feature_MostRecent] ([URL], [Feature])");
    }
}
