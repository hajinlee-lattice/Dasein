package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.MostRecentSource;

public abstract class AbstractMostRecentService extends AbstractRefreshService implements RefreshService {

    @Override
    public abstract MostRecentSource getSource();

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executeMergeRawData(getSource(), progress.getRootOperationUID());
    }

    @Override
    protected String workflowDirInHdfs(RefreshProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeys.MOST_RECENT_FLOW)
                .append(progress.getRootOperationUID()).toString();
    }

    @Override
    public String findBaseVersionForNewProgress() {
        CollectedSource baseSource = getSource().getBaseSources()[0];
        Date collectedLatest = hdfsSourceEntityMgr.getLatestTimestamp(baseSource);
        String targetVersion = HdfsPathBuilder.dateFormat.format(collectedLatest);
        if (getProgressEntityMgr().findProgressByBaseVersion(getSource(), targetVersion) == null) {
            return targetVersion;
        }
        return null;
    }

}
