package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.source.impl.FeaturePivoted;

@Component("featurePivotService")
public class FeaturePivotService extends AbstractPivotService implements PivotService {

    Log log = LogFactory.getLog(this.getClass());

    @Autowired
    ArchiveProgressEntityMgr archiveProgressEntityMgr;

    @Autowired
    RefreshProgressEntityMgr progressEntityMgr;

    @Autowired
    FeaturePivoted source;

    @Override
    public PivotedSource getSource() { return source; }

    @Override
    RefreshProgressEntityMgr getProgressEntityMgr() { return progressEntityMgr; }

    @Override
    Log getLogger() { return log; }

    @Override
    protected void createStageTable() {
        super.createStageTable();
        jdbcTemplateCollectionDB.execute(
                "CREATE CLUSTERED INDEX IX_URLFeature ON [" + getStageTableName() + "] ([URL])");
        jdbcTemplateCollectionDB.execute(
                "CREATE INDEX IX_Timtstamp ON [" + getStageTableName() + "] ([Timestamp])");
    }


}
