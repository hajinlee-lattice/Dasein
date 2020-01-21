package com.latticeengines.datacloud.collection.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.EmptyResultDataAccessException;

import com.latticeengines.datacloud.collection.entitymgr.ArchiveProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectedArchiveService;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.util.DateRange;
import com.latticeengines.datacloud.core.util.LoggingUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;

public abstract class AbstractCollectionArchiveService extends SourceRefreshServiceBase<ArchiveProgress>
        implements CollectedArchiveService {

    private static final Logger log = LoggerFactory.getLogger(AbstractCollectionArchiveService.class);

    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    @Override
    public abstract CollectedSource getSource();

    @Override
    public ArchiveProgress startNewProgress(Date startDate, Date endDate, String creator) {
        ArchiveProgress progress = getProgressEntityMgr().insertNewProgress(getSource(), startDate, endDate, creator);
        log.info(LoggingUtils.log(getClass().getSimpleName(), progress,
                "Started a new progress with StartDate=" + startDate + " endDate=" + endDate));
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
        getProgressEntityMgr().updateStatus(progress, ProgressStatus.DOWNLOADING);
        log.info(LoggingUtils.log(getClass().getSimpleName(), progress, "Start downloading ..."));

        // download incremental raw data and dest table snapshot
        if (!importIncrementalRawDataAndUpdateProgress(progress)) {
            return progress;
        }

        log.info(LoggingUtils.logWithDuration(getClass().getSimpleName(), progress, "Downloaded.", startTime));
        return getProgressEntityMgr().updateStatus(progress, ProgressStatus.DOWNLOADED);
    }

    @Override
    public DateRange determineNewJobDateRange() {
        Date end = getLatestTimestampUnarchived();
        Date start = getLatestTimestampArchived();
        return new DateRange(start, end);
    }

    @Override
    public ArchiveProgress finish(ArchiveProgress progress) {
        return finishProgress(progress);
    }

    private String incrementalDataDirInHdfs(ArchiveProgress progress) {
        Path incrementalDataDir = hdfsPathBuilder.constructRawIncrementalDir(getSource(), progress.getEndDate());
        return incrementalDataDir.toString();
    }

    private String constructWhereClauseByDates(String timestampColumn, Date startDate, Date endDate) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return String.format("\"%s >= '%s' AND %s <= '%s'\"", timestampColumn, dateFormat.format(startDate),
                timestampColumn, dateFormat.format(endDate));
    }

    private boolean importIncrementalRawDataAndUpdateProgress(ArchiveProgress progress) {
        String whereClause = constructWhereClauseByDates(getSource().getDownloadSplitColumn(), progress.getStartDate(),
                progress.getEndDate());

        Long rowsToDownload = jdbcTemplateCollectionDB
                .queryForObject("SELECT COUNT(*) FROM " + getSource().getCollectedTableName() + " WHERE "
                        + whereClause.substring(1, whereClause.lastIndexOf("\"")), Long.class);
        rowsToDownload = (rowsToDownload == null) ? 0L : rowsToDownload;
        if (rowsToDownload == 0) {
            progress.setRowsDownloadedToHdfs(rowsToDownload);
            return true;
        }

        Date earliest = jdbcTemplateCollectionDB.queryForObject(
                "SELECT MIN([" + getSource().getTimestampField() + "]) FROM " + getSource().getCollectedTableName()
                        + " WHERE " + whereClause.substring(1, whereClause.lastIndexOf("\"")).replace(">=", ">"),
                Date.class);

        log.info(LoggingUtils.log(getClass().getSimpleName(), progress, "Resolved StartDate=" + earliest));

        Date latest = jdbcTemplateCollectionDB.queryForObject(
                "SELECT MAX([" + getSource().getTimestampField() + "]) FROM " + getSource().getCollectedTableName()
                        + " WHERE " + whereClause.substring(1, whereClause.lastIndexOf("\"")).replace(">=", ">"),
                Date.class);

        log.info(LoggingUtils.log(getClass().getSimpleName(), progress, "Resolved EndDate=" + latest));

        progress.setStartDate(earliest);
        progress.setEndDate(latest);
        getProgressEntityMgr().updateProgress(progress);
        whereClause = constructWhereClauseByDates(getSource().getDownloadSplitColumn(), progress.getStartDate(),
                progress.getEndDate());

        rowsToDownload = jdbcTemplateCollectionDB
                .queryForObject("SELECT COUNT(*) FROM " + getSource().getCollectedTableName() + " WHERE "
                        + whereClause.substring(1, whereClause.lastIndexOf("\"")), Long.class);

        String targetDir = incrementalDataDirInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }

        if (rowsToDownload > 0 && !importFromCollectionDB(getSource().getCollectedTableName(), targetDir,
                getSource().getDownloadSplitColumn(), whereClause, progress)) {
            updateStatusToFailed(progress, "Failed to import incremental data from DB.", null);
            return false;
        }

        long rowsDownloaded = countSourceTable(progress);
        progress.setRowsDownloadedToHdfs(rowsDownloaded);

        hdfsSourceEntityMgr.setLatestTimestamp(getSource(), latest);

        return true;
    }

    protected Date getLatestTimestampUnarchived() {
        Date latestInSrc;
        try {
            latestInSrc = jdbcTemplateCollectionDB.queryForObject(
                    "SELECT MAX([" + getSource().getTimestampField() + "]) FROM " + getSource().getCollectedTableName(),
                    Date.class);
        } catch (EmptyResultDataAccessException e) {
            latestInSrc = new Date(System.currentTimeMillis());
        }
        return latestInSrc;
    }

    protected Date getLatestTimestampArchived() {
        Date latestInHdfs = hdfsSourceEntityMgr.getLatestTimestamp(getSource());
        if (latestInHdfs == null) {
            latestInHdfs = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3650));
        }
        return latestInHdfs;

    }

}
