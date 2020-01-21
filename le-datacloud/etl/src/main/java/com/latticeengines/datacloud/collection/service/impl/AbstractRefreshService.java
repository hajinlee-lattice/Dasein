package com.latticeengines.datacloud.collection.service.impl;

import java.io.File;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.collection.entitymgr.RefreshProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectionDataFlowKeysDeprecated;
import com.latticeengines.datacloud.collection.service.RefreshService;
import com.latticeengines.datacloud.core.source.DerivedSource;
import com.latticeengines.datacloud.core.source.DomainBased;
import com.latticeengines.datacloud.core.source.HasSqlPresence;
import com.latticeengines.datacloud.core.source.PurgeStrategy;
import com.latticeengines.datacloud.core.util.LoggingUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.RefreshProgress;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;

@SuppressWarnings("deprecation")
public abstract class AbstractRefreshService extends SourceRefreshServiceBase<RefreshProgress>
        implements RefreshService {

    private static final Logger log = LoggerFactory.getLogger(AbstractRefreshService.class);

    abstract RefreshProgressEntityMgr getProgressEntityMgr();

    @Override
    public abstract DerivedSource getSource();

    abstract void executeDataFlow(RefreshProgress progress);

    @Override
    public RefreshProgress startNewProgress(Date pivotDate, String baseSourceVersion, String creator) {
        RefreshProgress progress = getProgressEntityMgr().insertNewProgress(getSource(), pivotDate, creator);
        progress.setBaseSourceVersion(baseSourceVersion);
        log.info(LoggingUtils.log(getClass().getSimpleName(), progress,
                "Started a new progress with pivotDate=" + pivotDate));
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
        log.info(LoggingUtils.log(getClass().getSimpleName(), progress, "Start transforming ..."));

        // merge raw and snapshot, then output most recent records
        if (!transformInternal(progress)) {
            return progress;
        }

        log.info(LoggingUtils.logWithDuration(getClass().getSimpleName(), progress, "Transformed.", startTime));
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
        log.info(LoggingUtils.log(getClass().getSimpleName(), progress, "Start uploading ..."));

        if (getSource() instanceof HasSqlPresence) {
            // upload source
            long uploadStartTime = System.currentTimeMillis();
            String sourceDir = snapshotDirInHdfs(progress);
            String destTable = ((HasSqlPresence) getSource()).getSqlTableName();
            if (!uploadAvroToCollectionDB(progress, sourceDir, destTable)) {
                return progress;
            }

            Long rowsUploaded = jdbcTemplateCollectionDB.queryForObject("SELECT COUNT(*) FROM " + destTable,
                    Long.class);
            rowsUploaded = (rowsUploaded == null) ? 0L : rowsUploaded;
            progress.setRowsUploadedToSql(rowsUploaded);
            log.info(LoggingUtils.logWithDuration(getClass().getSimpleName(), progress,
                    "Uploaded " + rowsUploaded + " rows to " + destTable, uploadStartTime));
        }

        // finish
        log.info(LoggingUtils.logWithDuration(getClass().getSimpleName(), progress, "Uploaded.", startTime));

        return getProgressEntityMgr().updateStatus(progress, ProgressStatus.UPLOADED);
    }

    @Override
    public RefreshProgress finish(RefreshProgress progress) {
        log.info(LoggingUtils.log(getClass().getSimpleName(),
                String.format("Refreshing %s successful, generated Rows=%d", progress.getSourceName(),
                        progress.getRowsGeneratedInHdfs())));
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
            if (versions.size() <= numToKeep) {
                return;
            }

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
            if (!HdfsUtils.fileExists(yarnConfiguration, snapshotDir + "/_SUCCESS")) {
                HdfsUtils.writeToFile(yarnConfiguration, snapshotDir + "/_SUCCESS", "");
            }
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
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeysDeprecated.TRANSFORM_FLOW)
                .append(progress.getRootOperationUID()).toString();
    }

    protected boolean uploadAvroToCollectionDB(RefreshProgress progress, String avroDir, String destTable) {
        String stageTableName = getStageTableName();
        String bakTableName = destTable + "_bak";

        try {
            log.info(LoggingUtils.log(getClass().getSimpleName(), progress,
                    "Create a clean stage table " + stageTableName));
            dropJdbcTableIfExists(stageTableName);
            createStageTable();

            SqoopExporter exporter = getCollectionDbExporter(stageTableName, avroDir);
            ApplicationId appId = ConverterUtils
                    .toApplicationId(sqoopProxy.exportData(exporter).getApplicationIds().get(0));
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId, 24 * 3600);
            if (!FinalApplicationStatus.SUCCEEDED.equals(status)) {
                throw new IllegalStateException("The final state of " + appId + " is not "
                        + FinalApplicationStatus.SUCCEEDED + " but rather " + status);
            }

            log.info(LoggingUtils.log(getClass().getSimpleName(), progress,
                    "Creating indices on the stage table " + stageTableName));
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
        log.info(LoggingUtils.log(getClass().getSimpleName(), progress,
                String.format("Rename %s to %s.", srcTable, destTable)));
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
