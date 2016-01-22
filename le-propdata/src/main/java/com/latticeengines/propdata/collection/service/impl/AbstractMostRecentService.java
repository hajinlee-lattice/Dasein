package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;

import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.MostRecentSource;

public abstract class AbstractMostRecentService extends AbstractRefreshService implements RefreshService {

    @Override
    public abstract MostRecentSource getSource();

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executeMergeRawData(
                getSource(),
                progress.getRootOperationUID()
        );
    }

    @Override
    protected void createStageTable() {
        String sql = "SELECT TOP 0 * INTO [" + getStageTableName() + "] FROM ["
                + rawCollectedSourceTableName(getSource()) + "]";
        jdbcTemplateCollectionDB.execute(sql);
    }

    private String rawCollectedSourceTableName(MostRecentSource source) {
        return source.getBaseSources()[0].getCollectedTableName();
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
        Date archivedLatest = archivedLatest();
        return collectedLatest.after(archivedLatest) ? HdfsPathBuilder.dateFormat.format(collectedLatest) : null;
    }

    private Date archivedLatest() {
        Date latest = jdbcTemplateCollectionDB.queryForObject(
                "SELECT MAX([" + getSource().getTimestampField() + "]) FROM "
                        + ((HasSqlPresence) getSource()).getSqlTableName(),
                Date.class);
        return latest;
    }
}
