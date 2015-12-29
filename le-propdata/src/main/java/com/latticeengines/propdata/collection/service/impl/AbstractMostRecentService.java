package com.latticeengines.propdata.collection.service.impl;

import org.apache.commons.lang.StringUtils;

import com.latticeengines.domain.exposed.propdata.collection.RefreshProgress;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.collection.source.MostRecentSource;

public abstract class AbstractMostRecentService extends AbstractRefreshService implements RefreshService {

    @Override
    abstract MostRecentSource getSource();

    @Override
    protected void executeDataFlow(RefreshProgress progress) {
        collectionDataFlowService.executeMergeRawData(
                getSource(),
                progress.getRootOperationUID()
        );
    }

    @Override
    protected String createStageTableSql() {
        String sql = "SELECT TOP 0 * INTO [" + getStageTableName() + "] FROM ["
                + rawCollectedSourceTableName(getSource()) + "] \n";
        sql += " \n CREATE CLUSTERED INDEX IX_PKS ON [" + getStageTableName() + "] ( [";
        sql += StringUtils.join(getSource().getPrimaryKey(), "], [");
        sql += "] ) \n";
        sql += " CREATE INDEX IX_TIME ON [" + getStageTableName() + "] ( [";
        sql += getSource().getTimestampField() + "] )";
        return sql;
    }

    private String rawCollectedSourceTableName(MostRecentSource source) {
        return source.getBaseSource().getCollectedTableName();
    }

    @Override
    protected String workflowDirInHdfs(RefreshProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeys.MERGE_RAW_FLOW)
                .append(progress.getRootOperationUID()).toString();
    }
}
