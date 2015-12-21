package com.latticeengines.propdata.collection.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.springframework.dao.EmptyResultDataAccessException;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.source.CollectionSource;
import com.latticeengines.propdata.collection.util.DateRange;
import com.latticeengines.propdata.collection.util.LoggingUtils;

public abstract class AbstractArchiveService extends AbstractSourceRefreshService<ArchiveProgress> implements ArchiveService {

    private Log log;
    private ArchiveProgressEntityMgr entityMgr;
    private CollectionSource source;

    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    abstract CollectionSource getSource();

    abstract String getSourceTableName();

    abstract String getMergeDataFlowQualifier();

    abstract String getSrcTableSplitColumn();

    abstract String getSrcTableTimestampColumn();

    abstract String createIndexForStageTableSql();

    @PostConstruct
    private void setEntityMgrs() {
        source = getSource();
        entityMgr = getProgressEntityMgr();
        log = getLogger();
    }

    @Override
    public ArchiveProgress startNewProgress(Date startDate, Date endDate, String creator) {
        ArchiveProgress progress = entityMgr.insertNewProgress(source, startDate, endDate, creator);
        LoggingUtils.logInfo(log, progress, "Started a new progress with StartDate=" + startDate
                + " endDate=" + endDate);
        return progress;
    }

    @Override
    public ArchiveProgress importFromDB(ArchiveProgress progress) {
        // check request context
        if (!checkProgressStatus(progress, ProgressStatus.NEW, ProgressStatus.DOWNLOADING)) {
            return progress;
        }

        // update status
        logIfRetrying(progress);
        long startTime = System.currentTimeMillis();
        entityMgr.updateStatus(progress, ProgressStatus.DOWNLOADING);
        LoggingUtils.logInfo(log, progress, "Start downloading ...");

        // download incremental raw data and dest table snapshot
        if (!importIncrementalRawDataAndUpdateProgress(progress)) {
            return progress;
        }

        LoggingUtils.logInfoWithDuration(log, progress, "Downloaded.", startTime);
        progress.setNumRetries(0);
        return entityMgr.updateStatus(progress, ProgressStatus.DOWNLOADED);
    }

    @Override
    public ArchiveProgress transformRawData(ArchiveProgress progress) {
        // check request context
        if (!checkProgressStatus(progress, ProgressStatus.DOWNLOADED, ProgressStatus.TRANSFORMING)) {
            return progress;
        }

        // update status
        long startTime = System.currentTimeMillis();
        logIfRetrying(progress);
        entityMgr.updateStatus(progress, ProgressStatus.TRANSFORMING);
        LoggingUtils.logInfo(log, progress, "Start transforming ...");

        // merge raw and snapshot, then output most recent records
        if (progress.getRowsDownloadedToHdfs() > 0 && !transformRawDataInternal(progress)) {
            return progress;
        }

        LoggingUtils.logInfoWithDuration(log, progress, "Transformed.", startTime);
        progress.setNumRetries(0);
        return entityMgr.updateStatus(progress, ProgressStatus.TRANSFORMED);
    }


    @Override
    public ArchiveProgress exportToDB(ArchiveProgress progress) {
        // check request context
        if (!checkProgressStatus(progress, ProgressStatus.TRANSFORMED, ProgressStatus.UPLOADING)) {
            return progress;
        }

        // update status
        long startTime = System.currentTimeMillis();
        logIfRetrying(progress);
        entityMgr.updateStatus(progress, ProgressStatus.UPLOADING);
        LoggingUtils.logInfo(log, progress, "Start uploading ...");

        if (progress.getRowsDownloadedToHdfs() <= 0) {
            LoggingUtils.logInfoWithDuration(log, progress, "Uploaded.", startTime);
            progress.setNumRetries(0);
            return entityMgr.updateStatus(progress, ProgressStatus.UPLOADED);
        }

        // upload source
        long uploadStartTime = System.currentTimeMillis();
        String sourceDir = snapshotDirInHdfs(progress);
        String destTable = source.getTableName();
        if (!uploadAvroToCollectionDB(progress, sourceDir, destTable, createIndexForStageTableSql())) {
            return progress;
        }

        long rowsUploaded = jdbcTemplateCollectionDB.queryForObject("SELECT COUNT(*) FROM " + destTable, Long.class);
        progress.setRowsUploadedToSql(rowsUploaded);
        LoggingUtils.logInfoWithDuration(getLogger(),
                progress, "Uploaded " + rowsUploaded + " rows to " + destTable, uploadStartTime);

        // finish
        LoggingUtils.logInfoWithDuration(log, progress, "Uploaded.", startTime);
        progress.setNumRetries(0);
        return entityMgr.updateStatus(progress, ProgressStatus.UPLOADED);
    }

    @Override
    public DateRange determineNewJobDateRange() {
        Date latestInSrc, latestInDest;
        try {
            latestInSrc = jdbcTemplateCollectionDB.queryForObject(
                    "SELECT TOP 1 " + getSrcTableTimestampColumn() + " FROM " + getSourceTableName()
                            + " ORDER BY " + getSrcTableTimestampColumn() + " DESC", Date.class);
        } catch (EmptyResultDataAccessException e) {
            latestInSrc = new Date(System.currentTimeMillis());
        }

        try {
            latestInDest = jdbcTemplateCollectionDB.queryForObject(
                    "SELECT TOP 1 " + getSrcTableTimestampColumn() + " FROM " + getDestTableName()
                            + " ORDER BY " + getSrcTableTimestampColumn() + " DESC", Date.class);
        } catch (EmptyResultDataAccessException e) {
            latestInDest = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3650));
        }

        return new DateRange(latestInDest, latestInSrc);
    }

    @Override
    public ArchiveProgress findJobToRetry() { return super.findJobToRetry(); }

    @Override
    public ArchiveProgress findRunningJob() { return super.findRunningJob(); }

    @Override
    public ArchiveProgress finish(ArchiveProgress progress) { return finishProgress(progress); }

    private String incrementalDataDirInHdfs(ArchiveProgress progress) {
        Path incrementalDataDir = hdfsPathBuilder.constructRawIncrementalDir(source, progress.getEndDate());
        return incrementalDataDir.toString();
    }

    private String constructWhereClauseByDates(String timestampColumn, Date startDate, Date endDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return String.format("\"%s > '%s' AND %s <= '%s'\"", timestampColumn, dateFormat.format(startDate),
                timestampColumn, dateFormat.format(endDate));
    }


    private boolean importIncrementalRawDataAndUpdateProgress(ArchiveProgress progress) {
        String targetDir = incrementalDataDirInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }

        String whereClause = constructWhereClauseByDates(getSrcTableTimestampColumn(),
                progress.getStartDate(), progress.getEndDate());
        String customer = getSqoopCustomerName(progress) + "-downloadRawData" ;

        if (!importFromCollectionDB(getSourceTableName(), targetDir, customer, getSrcTableSplitColumn(),
                whereClause, progress)) {
            updateStatusToFailed(progress, "Failed to import incremental data from DB.", null);
            return false;
        }

        long rowsDownloaded = jdbcTemplateCollectionDB.queryForObject("SELECT COUNT(*) FROM " + getSourceTableName()
                + " WHERE " + whereClause.substring(1, whereClause.lastIndexOf("\"")), Long.class);
        progress.setRowsDownloadedToHdfs(rowsDownloaded);

        return true;
    }

    private boolean transformRawDataInternal(ArchiveProgress progress) {
        // dedupe
        String targetDir = workflowDirInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }
        try {
            collectionDataFlowService.executeMergeRawSnapshotData(
                    source,
                    getMergeDataFlowQualifier(),
                    progress.getRootOperationUID()
            );
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform raw data.", e);
            return false;
        }

        // copy deduped result to snapshot
        try {
            String snapshotDir = snapshotDirInHdfs(progress);
            String srcDir = workflowDirInHdfs(progress);
            HdfsUtils.rmdir(yarnConfiguration, snapshotDir);
            HdfsUtils.copyFiles(yarnConfiguration, srcDir, snapshotDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy deduped data to Snapshot folder.", e);
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
            hdfsSourceEntityMgr.setCurrentVersion(source, getVersionString(progress));
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
            return false;
        }

        // extract schema
        try {
            extractSchema(progress);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to extract schema of " + source.getSourceName() + " avsc.", e);
            return false;
        }
        return true;
    }

    private String workflowDirInHdfs(ArchiveProgress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeys.MERGE_RAW_FLOW)
                .append(progress.getRootOperationUID()).toString();
    }

}
