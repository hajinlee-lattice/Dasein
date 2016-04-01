package com.latticeengines.propdata.collection.service.impl;

import java.io.File;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.domain.exposed.propdata.manage.RefreshProgress;
import com.latticeengines.propdata.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.RefreshService;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.DomainBased;
import com.latticeengines.propdata.core.source.HasSqlPresence;
import com.latticeengines.propdata.core.source.PurgeStrategy;
import com.latticeengines.propdata.core.util.LoggingUtils;

public abstract class AbstractRefreshService extends SourceRefreshServiceBase<RefreshProgress>
        implements RefreshService {

    abstract RefreshProgressEntityMgr getProgressEntityMgr();

    @Override
    public abstract DerivedSource getSource();

    abstract void executeDataFlow(RefreshProgress progress);

    @Override
    public RefreshProgress startNewProgress(Date pivotDate, String baseSourceVersion, String creator) {
        RefreshProgress progress = getProgressEntityMgr().insertNewProgress(getSource(), pivotDate, creator);
        progress.setBaseSourceVersion(baseSourceVersion);
        LoggingUtils.logInfo(getLogger(), progress, "Started a new progress with pivotDate=" + pivotDate);
        getProgressEntityMgr().updateStatus(progress, ProgressStatus.NEW);
        return progress;
    }

    @Override
    public RefreshProgress transform(RefreshProgress progress) {
        // check request context
        if (!checkProgressStatus(progress, ProgressStatus.NEW, ProgressStatus.TRANSFORMING)) {
            return progress;
        }

        // update status
        long startTime = System.currentTimeMillis();
        logIfRetrying(progress);
        getProgressEntityMgr().updateStatus(progress, ProgressStatus.TRANSFORMING);
        LoggingUtils.logInfo(getLogger(), progress, "Start transforming ...");

        // merge raw and snapshot, then output most recent records
        if (!transformInternal(progress)) {
            return progress;
        }

        LoggingUtils.logInfoWithDuration(getLogger(), progress, "Transformed.", startTime);
        return getProgressEntityMgr().updateStatus(progress, ProgressStatus.TRANSFORMED);
    }

    @Override
    public RefreshProgress exportToDB(RefreshProgress progress) {
        // check request context
        if (!checkProgressStatus(progress, ProgressStatus.TRANSFORMED, ProgressStatus.UPLOADING)) {
            return progress;
        }

        // update status
        long startTime = System.currentTimeMillis();
        logIfRetrying(progress);
        getProgressEntityMgr().updateStatus(progress, ProgressStatus.UPLOADING);
        LoggingUtils.logInfo(getLogger(), progress, "Start uploading ...");

        if (getSource() instanceof HasSqlPresence) {
            // upload source
            long uploadStartTime = System.currentTimeMillis();
            String sourceDir = snapshotDirInHdfs(progress);
            String destTable = ((HasSqlPresence) getSource()).getSqlTableName();
            if (!uploadAvroToCollectionDB(progress, sourceDir, destTable)) {
                return progress;
            }

            long rowsUploaded = jdbcTemplateCollectionDB.queryForObject("SELECT COUNT(*) FROM " + destTable,
                    Long.class);
            progress.setRowsUploadedToSql(rowsUploaded);
            LoggingUtils.logInfoWithDuration(getLogger(), progress,
                    "Uploaded " + rowsUploaded + " rows to " + destTable, uploadStartTime);
        }

        // finish
        LoggingUtils.logInfoWithDuration(getLogger(), progress, "Uploaded.", startTime);
        return getProgressEntityMgr().updateStatus(progress, ProgressStatus.UPLOADED);
    }

    @Override
    public RefreshProgress finish(RefreshProgress progress) {
        getLogger().info(String.format("Refreshing %s successful, generated Rows=%d", progress.getSourceName(),
                progress.getRowsGeneratedInHdfs()));
        return finishProgress(progress);
    }

    @Override
    public void purgeOldVersions() {
        PurgeStrategy purgeStrategy = getSource().getPurgeStrategy();
        if (PurgeStrategy.NEVER.equals(purgeStrategy)) {
            return;
        }

        List<String> versions = hdfsSourceEntityMgr.getVersions(getSource());

        if (PurgeStrategy.NUM_VERSIONS.equals(purgeStrategy)) {
            int numToKeep = getSource().getNumberOfVersionsToKeep();
            if (versions.size() <= numToKeep) { return; }

            Collections.sort(versions);
            Collections.reverse(versions);
            for (int i = numToKeep; i < versions.size(); i++) {
                hdfsSourceEntityMgr.purgeSourceAtVersion(getSource(), versions.get(i));
            }

        }
    }

    private boolean transformInternal(RefreshProgress progress) {
        String targetDir = workflowDirInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }
        try {
            executeDataFlow(progress);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform raw data.", e);
            return false;
        }

        // copy result to snapshot
        try {
            String snapshotDir = snapshotDirInHdfs(progress);
            String srcDir = workflowDirInHdfs(progress);
            HdfsUtils.rmdir(yarnConfiguration, snapshotDir);
            HdfsUtils.copyFiles(yarnConfiguration, srcDir, snapshotDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
            return false;
        }

        // delete intermediate data
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                HdfsUtils.rmdir(yarnConfiguration, targetDir);
            }
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to delete intermediate data.", e);
            return false;
        }

        // update current version
        try {
            hdfsSourceEntityMgr.setCurrentVersion(getSource(), getVersionString(progress));
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
            return false;
        }

        // extract schema
        try {
            extractSchema(progress);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to extract schema of " + getSource().getSourceName() + " avsc.", e);
            return false;
        }

        // count output
        try {
            Long count = countSourceTable(progress);
            progress.setRowsGeneratedInHdfs(count);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to count generated rows " + getSource().getSourceName(), e);
            return false;
        }
        return true;
    }

    protected String workflowDirInHdfs(RefreshProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeys.TRANSFORM_FLOW)
                .append(progress.getRootOperationUID()).toString();
    }

    protected boolean uploadAvroToCollectionDB(RefreshProgress progress, String avroDir, String destTable) {
        String stageTableName = getStageTableName();
        String bakTableName = destTable + "_bak";

        try {
            LoggingUtils.logInfo(getLogger(), progress, "Create a clean stage table " + stageTableName);
            dropJdbcTableIfExists(stageTableName);
            createStageTable();

            SqoopExporter exporter = getCollectionDbExporter(stageTableName, avroDir);
            ApplicationId appId = sqoopService.exportTable(exporter);
            FinalApplicationStatus status =
                    YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId, 24 * 3600);
            if (!FinalApplicationStatus.SUCCEEDED.equals(status)) {
                throw new IllegalStateException("The final state of " + appId + " is not "
                        + FinalApplicationStatus.SUCCEEDED + " but rather " + status);
            }

            LoggingUtils.logInfo(getLogger(), progress, "Creating indices on the stage table " + stageTableName);
            createIndicesOnStageTable();
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to upload " + destTable + " to DB.", e);
            return false;
        } finally {
            FileUtils.deleteQuietly(new File(stageTableName + ".java"));
        }

        try {
            swapTableNamesInDestDB(progress, destTable, bakTableName);
            swapTableNamesInDestDB(progress, stageTableName, destTable);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to swap stage and dest tables for " + destTable, e);
            swapTableNamesInDestDB(progress, bakTableName, destTable);

            return false;
        }

        return true;
    }

    private void dropJdbcTableIfExists(String tableName) {
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) DROP TABLE " + tableName);
    }

    private void swapTableNamesInDestDB(RefreshProgress progress, String srcTable, String destTable) {
        dropJdbcTableIfExists(destTable);
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + srcTable + "') AND type in (N'U')) EXEC sp_rename '" + srcTable + "', '" + destTable + "'");
        LoggingUtils.logInfo(getLogger(), progress, String.format("Rename %s to %s.", srcTable, destTable));
    }

    protected String getStageTableName() {
        return ((HasSqlPresence) getSource()).getSqlTableName() + "_stage";
    }

    protected void createStageTable() {
        String[] statements = sourceColumnEntityMgr.generateCreateTableSqlStatements(getSource(), getStageTableName());
        for (String statement : statements) {
            jdbcTemplateCollectionDB.execute(statement);
        }
    }

    protected void createIndicesOnStageTable() {
        jdbcTemplateCollectionDB.execute("CREATE INDEX IX_TIMESTAMP ON [" + getStageTableName() + "] " + "(["
                + getSource().getTimestampField() + "])");

        if (getSource() instanceof DomainBased) {
            DomainBased domainBased = (DomainBased) getSource();
            jdbcTemplateCollectionDB.execute("CREATE INDEX IX_DOMAIN ON [" + getStageTableName() + "] " + "(["
                    + domainBased.getDomainField() + "])");
        }

    }

}
