package com.latticeengines.propdata.collection.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import org.springframework.dao.EmptyResultDataAccessException;

import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectedArchiveService;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.util.DateRange;
import com.latticeengines.propdata.core.util.LoggingUtils;

public abstract class AbstractCollectionArchiveService
        extends SourceRefreshServiceBase<ArchiveProgress> implements CollectedArchiveService {


    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    @Override
    public abstract CollectedSource getSource();

    @Override
    public ArchiveProgress startNewProgress(Date startDate, Date endDate, String creator) {
        ArchiveProgress progress = getProgressEntityMgr().insertNewProgress(getSource(), startDate, endDate, creator);
        LoggingUtils.logInfo(getLogger(), progress, "Started a new progress with StartDate=" + startDate
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
        getProgressEntityMgr().updateStatus(progress, ProgressStatus.DOWNLOADING);
        LoggingUtils.logInfo(getLogger(), progress, "Start downloading ...");

        // download incremental raw data and dest table snapshot
        if (!importIncrementalRawDataAndUpdateProgress(progress)) {
            return progress;
        }

        LoggingUtils.logInfoWithDuration(getLogger(), progress, "Downloaded.", startTime);
        progress.setNumRetries(0);
        return getProgressEntityMgr().updateStatus(progress, ProgressStatus.DOWNLOADED);
    }

    @Override
    public DateRange determineNewJobDateRange() {
        Date end = getLatestTimestampUnarchived();
        Date start = getLatestTimestampArchived();
        return new DateRange(start, end);
    }

    @Override
    public ArchiveProgress finish(ArchiveProgress progress) { return finishProgress(progress); }

    private String incrementalDataDirInHdfs(ArchiveProgress progress) {
        Path incrementalDataDir = hdfsPathBuilder.constructRawIncrementalDir(getSource(), progress.getEndDate());
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

        String whereClause = constructWhereClauseByDates(getSource().getDownloadSplitColumn(),
                progress.getStartDate(), progress.getEndDate());
        String customer = getSqoopCustomerName(progress) + "-downloadRawData" ;

        Date earlist = jdbcTemplateCollectionDB.queryForObject(
                "SELECT MIN([" + getSource().getTimestampField() + "]) FROM "
                        + getSource().getCollectedTableName() + " WHERE "
                        + whereClause.substring(1, whereClause.lastIndexOf("\"")),
                Date.class);

        Date latest = jdbcTemplateCollectionDB.queryForObject(
                "SELECT MAX([" + getSource().getTimestampField() + "]) FROM "
                        + getSource().getCollectedTableName() + " WHERE "
                        + whereClause.substring(1, whereClause.lastIndexOf("\"")),
                Date.class);

        progress.setStartDate(earlist);
        progress.setEndDate(latest);
        getProgressEntityMgr().updateProgress(progress);
        whereClause = constructWhereClauseByDates(getSource().getDownloadSplitColumn(),
                progress.getStartDate(), progress.getEndDate());

        if (!importFromCollectionDB(getSource().getCollectedTableName(), targetDir, customer, getSource().getDownloadSplitColumn(),
                whereClause, progress)) {
            updateStatusToFailed(progress, "Failed to import incremental data from DB.", null);
            return false;
        }

        long rowsDownloaded = jdbcTemplateCollectionDB.queryForObject("SELECT COUNT(*) FROM "
                + getSource().getCollectedTableName() + " WHERE "
                + whereClause.substring(1, whereClause.lastIndexOf("\"")), Long.class);
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
