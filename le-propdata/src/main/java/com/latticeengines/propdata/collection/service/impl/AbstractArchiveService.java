package com.latticeengines.propdata.collection.service.impl;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.propdata.collection.source.CollectionSource;
import com.latticeengines.propdata.collection.util.DateRange;
import com.latticeengines.propdata.collection.util.LoggingUtils;

public abstract class AbstractArchiveService extends AbstractSourceRefreshService implements ArchiveService {

    private Log log;
    private ArchiveProgressEntityMgr entityMgr;
    private CollectionSource source;

    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    abstract Log getLogger();

    abstract CollectionSource getSource();

    abstract String getSourceTableName();

    abstract String getMergeDataFlowQualifier();

    abstract String getSrcTableSplitColumn();

    abstract String getDestTableSplitColumn();

    abstract String getSrcTableTimestampColumn();

    abstract String createIndexForStageTableSql();

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected CollectionDataFlowService collectionDataFlowService;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplate;

    @Value("${propdata.collection.host}")
    private String dbHost;

    @Value("${propdata.collection.port}")
    private int dbPort;

    @Value("${propdata.collection.db}")
    private String db;

    @Value("${propdata.collection.user}")
    private String dbUser;

    @Value("${propdata.collection.password.encrypted}")
    private String dbPassword;

    @Value("${propdata.collection.sqoop.mapper.number}")
    private int numMappers;

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

        if (progress.getRowsDownloadedToHdfs() > 0 && !importSnapshotDestData(progress)) {
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
        String sourceDir = snapshotDirInHdfs();
        String destTable = source.getTableName();
        if (!uploadAvroToDestTable(progress, sourceDir, destTable, createIndexForStageTableSql())) {
            return progress;
        }

        // finish
        LoggingUtils.logInfoWithDuration(log, progress, "Uploaded.", startTime);
        progress.setNumRetries(0);
        return entityMgr.updateStatus(progress, ProgressStatus.UPLOADED);
    }

    @Override
    public ArchiveProgress findJobToRetry() {
        return entityMgr.findEarliestFailureUnderMaxRetry(source);
    }

    @Override
    public ArchiveProgress findRunningJob() {
        return entityMgr.findProgressNotInFinalState(source);
    }

    @Override
    public DateRange determineNewJobDateRange() {
        Date latestInSrc, latestInDest;
        try {
            latestInSrc = jdbcTemplate.queryForObject(
                    "SELECT TOP 1 " + getSrcTableTimestampColumn() + " FROM " + getSourceTableName()
                            + " ORDER BY " + getSrcTableTimestampColumn() + " DESC", Date.class);
        } catch (EmptyResultDataAccessException e) {
            latestInSrc = new Date(System.currentTimeMillis());
        }

        try {
            latestInDest = jdbcTemplate.queryForObject(
                    "SELECT TOP 1 " + getSrcTableTimestampColumn() + " FROM " + getDestTableName()
                            + " ORDER BY " + getSrcTableTimestampColumn() + " DESC", Date.class);
        } catch (EmptyResultDataAccessException e) {
            latestInDest = new Date(System.currentTimeMillis() - TimeUnit.DAYS.toMillis(3650));
        }

        return new DateRange(latestInDest, latestInSrc);
    }

    private String incrementalDataDirInHdfs(ArchiveProgress progress) {
        Path incrementalDataDir = hdfsPathBuilder.constructRawDataFlowIncrementalDir(source, progress.getEndDate());
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

        if (!importCollectionDBBySqoop(getSourceTableName(), targetDir, customer, getSrcTableSplitColumn(),
                whereClause, progress)) {
            updateStatusToFailed(progress, "Failed to import incremental data from DB.", null);
            return false;
        }

        long rowsDownloaded = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + getSourceTableName()
                + " WHERE " + whereClause.substring(1, whereClause.lastIndexOf("\"")), Long.class);
        progress.setRowsDownloadedToHdfs(rowsDownloaded);

        return true;
    }


    private boolean importSnapshotDestData(ArchiveProgress progress) {
        String targetDir = snapshotDirInHdfs();
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }

        String customer = getSqoopCustomerName(progress) + "-downloadSnapshotData" ;

        if (!importCollectionDBBySqoop(getDestTableName(), targetDir, customer,
                getDestTableSplitColumn(), null, progress)) {
            updateStatusToFailed(progress, "Failed to import snapshot data from DB.", null);
            return false;
        }

        return true;
    }

    private boolean transformRawDataInternal(ArchiveProgress progress) {
        // dedupe
        String targetDir = mergeWorkflowDirInHdfs();
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }
        try {
            collectionDataFlowService.executeMergeRawSnapshotData(
                    source,
                    incrementalDataDirInHdfs(progress),
                    getMergeDataFlowQualifier()
            );
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform raw data.", e);
            return false;
        }

        // copy deduped result to snapshot
        try {
            String snapshotDir = snapshotDirInHdfs();
            String srcDir = mergeWorkflowDirInHdfs() + "/Output";
            HdfsUtils.rmdir(yarnConfiguration, snapshotDir);
            HdfsUtils.copyFiles(yarnConfiguration, srcDir, snapshotDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy deduped data to Snapshot folder.", e);
            return false;
        }

        // extract schema
        try {
            extractSchema();
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to extract schema of " + source.getSourceName() + " avsc.", e);
            return false;
        }
        return true;
    }

}
