package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;

import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.BulkArchiveService;
import com.latticeengines.propdata.core.source.BulkSource;
import com.latticeengines.propdata.core.source.StageServer;
import com.latticeengines.propdata.core.util.LoggingUtils;

public abstract class AbstractBulkArchiveService
        extends SourceRefreshServiceBase<ArchiveProgress> implements BulkArchiveService {

    private Log log;
    private ArchiveProgressEntityMgr entityMgr;
    private BulkSource source;

    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    @Override
    public abstract BulkSource getSource();

    abstract String getSrcTableSplitColumn();

    @PostConstruct
    private void setEntityMgrs() {
        source = getSource();
        entityMgr = getProgressEntityMgr();
        log = getLogger();
    }

    @Override
    public ArchiveProgress startNewProgress(Date startDate, Date endDate, String creator) {
        return startNewProgress(creator);
    }

    @Override
    public ArchiveProgress startNewProgress(String creator) {
        ArchiveProgress progress = entityMgr.insertNewProgress(source, null, null, creator);
        LoggingUtils.logInfo(log, progress, "Started a new progress");
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
        if (!importBulkRawDataAndUpdateProgress(progress)) {
            return progress;
        }


        LoggingUtils.logInfoWithDuration(log, progress, "Downloaded.", startTime);
        progress.setNumRetries(0);
        return entityMgr.updateStatus(progress, ProgressStatus.DOWNLOADED);
    }

    @Override
    public ArchiveProgress finish(ArchiveProgress progress) { return finishProgress(progress); }

    private boolean importBulkRawDataAndUpdateProgress(ArchiveProgress progress) {
        String targetDir = snapshotDirInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }
        String customer = getSqoopCustomerName(progress) + "-downloadRawData" ;

        if (StageServer.COLLECTION_DB.equals(getSource().getBulkStageServer())) {
            if (!importFromCollectionDB(getSource().getBulkStageTableName(), targetDir, customer, getSrcTableSplitColumn(),
                    null, progress)) {
                updateStatusToFailed(progress, "Failed to import incremental data from DB.", null);
                return false;
            }
        } else {
            return false;
        }

        long rowsDownloaded = jdbcTemplateCollectionDB.queryForObject("SELECT COUNT(*) FROM "
                + getSource().getBulkStageTableName(), Long.class);
        progress.setRowsDownloadedToHdfs(rowsDownloaded);

        // update current version
        try {
            hdfsSourceEntityMgr.setCurrentVersion(source, getVersionString(progress));
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
            return false;
        }

        return true;
    }

}
