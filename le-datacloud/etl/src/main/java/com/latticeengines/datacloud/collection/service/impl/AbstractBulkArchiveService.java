package com.latticeengines.datacloud.collection.service.impl;

import java.util.Date;

import javax.annotation.PostConstruct;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.BulkArchiveService;
import com.latticeengines.datacloud.core.source.BulkSource;
import com.latticeengines.datacloud.core.util.LoggingUtils;
import com.latticeengines.domain.exposed.datacloud.StageServer;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

public abstract class AbstractBulkArchiveService extends SourceRefreshServiceBase<ArchiveProgress>
        implements BulkArchiveService {

    private static final Logger log = LoggerFactory.getLogger(AbstractBulkArchiveService.class);

    private String beanName;
    private ArchiveProgressEntityMgr entityMgr;
    private BulkSource source;

    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    @Override
    public abstract BulkSource getSource();

    abstract String getSrcTableSplitColumn();

    @PostConstruct
    private void postConstruct() {
        source = getSource();
        entityMgr = getProgressEntityMgr();
        beanName = getBeanName();
    }

    @Override
    public ArchiveProgress startNewProgress(Date startDate, Date endDate, String creator) {
        return startNewProgress(creator);
    }

    @Override
    public ArchiveProgress startNewProgress(String creator) {
        ArchiveProgress progress = entityMgr.insertNewProgress(source, null, null, creator);
        log.info(LoggingUtils.log(beanName, progress, "Started a new progress"));
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
        log.info(LoggingUtils.log(beanName, progress, "Start downloading ..."));

        // download incremental raw data and dest table snapshot
        if (!importBulkRawDataAndUpdateProgress(progress)) {
            return progress;
        }

        log.info(LoggingUtils.logWithDuration(getClass().getSimpleName(), progress, "Downloaded.", startTime));
        return entityMgr.updateStatus(progress, ProgressStatus.DOWNLOADED);
    }

    @Override
    public ArchiveProgress finish(ArchiveProgress progress) {
        return finishProgress(progress);
    }

    private boolean importBulkRawDataAndUpdateProgress(ArchiveProgress progress) {
        String targetDir = snapshotDirInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }
        if (StageServer.COLLECTION_DB.equals(getSource().getBulkStageServer())) {
            if (!importFromCollectionDB(getSource().getBulkStageTableName(), targetDir, getSrcTableSplitColumn(), null,
                    progress)) {
                updateStatusToFailed(progress, "Failed to import bulk data from DB.", null);
                return false;
            }
        } else {
            return false;
        }

        // update current version
        try {
            hdfsSourceEntityMgr.setCurrentVersion(source, getVersionString(progress));
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
            return false;
        }

        long rowsDownloaded = countSourceTable(progress);
        progress.setRowsDownloadedToHdfs(rowsDownloaded);

        return true;
    }

}
