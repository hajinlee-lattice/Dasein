package com.latticeengines.propdata.collection.service.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.collection.FeatureArchiveProgress;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.FeatureArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.FeatureArchiveService;

@Component("featureArchiveService")
public class FeatureArchiveServiceImpl extends AbstractArchiveService<FeatureArchiveProgress>
        implements FeatureArchiveService {

    Logger log = LoggerFactory.getLogger(this.getClass());

    @Autowired
    FeatureArchiveProgressEntityMgr progressEntityMgr;

    @Override
    ArchiveProgressEntityMgr<FeatureArchiveProgress> getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Logger getLogger() { return log; }

    @Override
    String getSourceTableName() { return "Feature"; }

    @Override
    String getDestTableName() { return "Feature_MostRecent"; }

    @Override
    String getMergeDataFlowQualifier() { return "featureMergeRawSnapshotDataFlowBuilder"; }

    @Override
    String getSrcTableSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String getDestTableSplitColumn() { return "LE_Last_Upload_Date"; }

    @Override
    String getSrcTableTimestampColumn() { return "LE_Last_Upload_Date"; }
}
