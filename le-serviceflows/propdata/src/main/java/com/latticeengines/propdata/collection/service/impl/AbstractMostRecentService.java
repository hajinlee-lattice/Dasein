package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;

import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeysDeprecated;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.MostRecentSource;

@SuppressWarnings("deprecation")
public abstract class AbstractMostRecentService extends AbstractRefreshService implements RefreshService {

    @Override
    public abstract MostRecentSource getSource();

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executeMergeRawData(getSource(), progress.getRootOperationUID(), null);
    }

    @Override
    protected String workflowDirInHdfs(RefreshProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeysDeprecated.MOST_RECENT_FLOW)
                .append(progress.getRootOperationUID()).toString();
    }

    @Override
    public String findBaseVersionForNewProgress() {
        CollectedSource baseSource = (CollectedSource) getSource().getBaseSources()[0];
        Date collectedLatest = hdfsSourceEntityMgr.getLatestTimestamp(baseSource);
        String targetVersion = HdfsPathBuilder.dateFormat.format(collectedLatest);
        if (getProgressEntityMgr().findProgressByBaseVersion(getSource(), targetVersion) == null) {
            return targetVersion;
        }
        return null;
    }

}
