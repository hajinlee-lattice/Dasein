package com.latticeengines.propdata.collection.service.impl;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;

import javax.annotation.PostConstruct;

import com.latticeengines.common.exposed.util.AvroUtils;
import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.camille.Path;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressBase;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.ArchiveService;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.propdata.collection.service.CollectionJobContext;
import com.latticeengines.propdata.collection.util.LoggingUtils;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class AbstractArchiveService<Progress extends ArchiveProgressBase> implements ArchiveService {

    private Logger log;
    private ArchiveProgressEntityMgr<Progress> entityMgr;
    private SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");

    abstract ArchiveProgressEntityMgr<Progress> getProgressEntityMgr();

    abstract Logger getLogger();

    abstract String getSourceTableName();

    abstract String getDestTableName();

    abstract String getMergeDataFlowQualifier();

    abstract String getSrcTableSplitColumn();

    abstract String getDestTableSplitColumn();

    abstract String getSrcTableTimestampColumn();

    @Autowired
    private SqoopSyncJobService sqoopService;

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected CollectionDataFlowService collectionDataFlowService;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplateDest")
    protected JdbcTemplate jdbcTemplateDest;

    @Value("${propdata.collection.src.host}")
    private String srcHost;

    @Value("${propdata.collection.src.port}")
    private int srcPort;

    @Value("${propdata.collection.src.db}")
    private String srcDb;

    @Value("${propdata.collection.dest.host}")
    private String destHost;

    @Value("${propdata.collection.dest.port}")
    private int destPort;

    @Value("${propdata.collection.dest.db}")
    private String destDb;

    @Value("${propdata.collection.user}")
    private String dbUser;

    @Value("${propdata.collection.password.encrypted}")
    private String dbPassword;

    @Value("${propdata.collection.sqoop.mapper.number}")
    private int numMappers;

    @PostConstruct
    private void setEntityMgrs() {
        entityMgr = getProgressEntityMgr();
        log = getLogger();
    }

    @Override
    public CollectionJobContext startNewProgress(Date startDate, Date endDate, String creator) {
        try {
            Progress progress = entityMgr.insertNewProgress(startDate, endDate, creator);
            CollectionJobContext context = new CollectionJobContext();
            context.setProperty(CollectionJobContext.PROGRESS_KEY, progress);
            return context;
        } catch (InstantiationException|IllegalAccessException e) {
            throw new RuntimeException(
                    "Failed to create a new progress with StartDate=" + startDate + " endDate=" + endDate);
        }
    }

    @Override
    public CollectionJobContext importFromDB(CollectionJobContext request) {
        CollectionJobContext response = new CollectionJobContext();

        // check request context
        if (!checkProgressStatus(request, ArchiveProgressStatus.NEW, ArchiveProgressStatus.DOWNLOADING)) {
            return response;
        }
        Progress progress = request.getProperty(CollectionJobContext.PROGRESS_KEY, entityMgr.getProgressClass());
        response.setProperty(CollectionJobContext.PROGRESS_KEY, progress);

        // update status
        long startTime = System.currentTimeMillis();
        entityMgr.updateStatus(progress, ArchiveProgressStatus.DOWNLOADING);
        LoggingUtils.logInfo(log, progress, "Start downloading ...");

        // download incremental raw data and dest table snapshot
        if (!importIncrementalRawDataAndUpdateProgress(progress)) {
            response.setProperty(CollectionJobContext.PROGRESS_KEY, progress);
            return response;
        }
        if (progress.getRowsDownloadedToHdfs() > 0 && !importSnapshotDestData(progress)) {
            response.setProperty(CollectionJobContext.PROGRESS_KEY, progress);
            return response;
        }

        LoggingUtils.logInfoWithDuration(log, progress, "Downloaded.", startTime);
        return refreshContextAndDBToStatus(response, progress, ArchiveProgressStatus.DOWNLOADED);
    }

    @Override
    public CollectionJobContext transformRawData(CollectionJobContext request) {
        CollectionJobContext response = new CollectionJobContext();

        // check request context
        if (!checkProgressStatus(request, ArchiveProgressStatus.DOWNLOADED, ArchiveProgressStatus.TRANSFORMING)) {
            return response;
        }
        Progress progress = request.getProperty(CollectionJobContext.PROGRESS_KEY, entityMgr.getProgressClass());
        response.setProperty(CollectionJobContext.PROGRESS_KEY, progress);

        // update status
        long startTime = System.currentTimeMillis();
        entityMgr.updateStatus(progress, ArchiveProgressStatus.TRANSFORMING);
        LoggingUtils.logInfo(log, progress, "Start transforming ...");

        if (progress.getRowsDownloadedToHdfs() <= 0) {
            LoggingUtils.logInfoWithDuration(log, progress, "Transformed.", startTime);
            return refreshContextAndDBToStatus(response, progress, ArchiveProgressStatus.TRANSFORMED);
        }

        // merge raw and snapshot, then output most recent records
        if (!transformRawDataToMostRecent(progress)) {
            response.setProperty(CollectionJobContext.PROGRESS_KEY, progress);
            return response;
        }

        LoggingUtils.logInfoWithDuration(log, progress, "Transformed.", startTime);
        return refreshContextAndDBToStatus(response, progress, ArchiveProgressStatus.TRANSFORMED);
    }


    @Override
    public CollectionJobContext exportToDB(CollectionJobContext request) {
        CollectionJobContext response = new CollectionJobContext();

        // check request context
        if (!checkProgressStatus(request, ArchiveProgressStatus.TRANSFORMED, ArchiveProgressStatus.UPLOADING)) {
            return response;
        }

        // update status
        long startTime = System.currentTimeMillis();
        Progress progress = request.getProperty(CollectionJobContext.PROGRESS_KEY, entityMgr.getProgressClass());
        entityMgr.updateStatus(progress, ArchiveProgressStatus.UPLOADING);
        LoggingUtils.logInfo(log, progress, "Start uploading ...");

        if (progress.getRowsDownloadedToHdfs() <= 0) {
            LoggingUtils.logInfoWithDuration(log, progress, "Transformed.", startTime);
            return refreshContextAndDBToStatus(response, progress, ArchiveProgressStatus.UPLOADED);
        }

        String sourceDir = workflowDirsInHdfs(progress) + "/Output";
        String destTable = getDestTableName();

        // cleanup stage table
        String stageTableName = destTable + "_stage";
        String bakTableName = destTable + "_bak";
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        String customer = getSqoopCustomerName(progress);

        try {
            LoggingUtils.logInfo(log, progress, "Create a clean stage table.");
            dropJdbcTableIfExists(jdbcTemplateDest, stageTableName);
            jdbcTemplateDest.execute("SELECT TOP 0 * INTO " + stageTableName + " FROM " + destTable);

            long uploadStartTime = System.currentTimeMillis();
            DbCreds.Builder builder = new DbCreds.Builder();
            builder.host(destHost).port(destPort).db(destDb).user(dbUser).password(dbPassword);
            DbCreds creds = new DbCreds(builder);
            sqoopService.exportDataSync(stageTableName, sourceDir, creds, assignedQueue,
                    customer + "-uploadRawDataExportData", numMappers, null);
            long rowsUploaded = jdbcTemplateDest.queryForObject("SELECT COUNT(*) FROM " + stageTableName, Long.class);
            progress.setRowsUploadedToSql(rowsUploaded);
            LoggingUtils.logInfoWithDuration(log, progress, "Uploaded " + rowsUploaded + " rows to stage table.", uploadStartTime);
        } catch (Exception e) {
            LoggingUtils.logError(log, progress, "Failed to upload data to DB.", e);
            updateStautsToFailed(progress, "Failed to upload data to DB.");
            response.setProperty(CollectionJobContext.PROGRESS_KEY, progress);
            return response;
        } finally {
            FileUtils.deleteQuietly(new File(stageTableName+".java"));
        }

        try {
            LoggingUtils.logInfo(log, progress, "Rename dest table to back up table.");
            dropJdbcTableIfExists(jdbcTemplateDest, bakTableName);
            jdbcTemplateDest.execute("EXEC sp_rename '" + destTable + "', '" + bakTableName + "'");

            LoggingUtils.logInfo(log, progress, "Rename stage table to dest table.");
            dropJdbcTableIfExists(jdbcTemplateDest, destTable);
            jdbcTemplateDest.execute("EXEC sp_rename '" + stageTableName + "', '" + destTable + "'");
        } catch (Exception e) {
            LoggingUtils.logError(log, progress, "Failed to swap stage and dest tables", e);
            updateStautsToFailed(progress, "Failed to swap stage and dest tables");

            LoggingUtils.logInfo(log, progress, "Restore backup table to dest table.");
            dropJdbcTableIfExists(jdbcTemplateDest, destTable);
            jdbcTemplateDest.execute("EXEC sp_rename '" + bakTableName + "', '" + destTable + "'");

            response.setProperty(CollectionJobContext.PROGRESS_KEY, progress);
            return response;
        } finally {
            LoggingUtils.logInfo(log, progress, "Drop backup table");
            dropJdbcTableIfExists(jdbcTemplateDest, bakTableName);
        }

        // finish
        LoggingUtils.logInfoWithDuration(log, progress, "Uploaded.", startTime);
        return refreshContextAndDBToStatus(response, progress, ArchiveProgressStatus.UPLOADED);
    }

    private boolean checkProgressStatus(CollectionJobContext request,
                                        ArchiveProgressStatus expectedStatus, ArchiveProgressStatus inProgress) {
        Progress progress = request.getProperty(CollectionJobContext.PROGRESS_KEY, entityMgr.getProgressClass());
        if (progress == null) { return false; }

        if (progress.getStatus().equals(inProgress)) {
            return false;
        }

        if (!progress.getStatus().equals(expectedStatus)) {
            LoggingUtils.logError(log, progress, "Progress is not in the status " + expectedStatus + " but rather " +
                    progress.getStatus() + " before "
                    + inProgress + ".", new IllegalStateException());
            return false;
        }

        return true;
    }

    private CollectionJobContext refreshContextAndDBToStatus(CollectionJobContext response,
                                                             Progress progress,
                                                             ArchiveProgressStatus status) {
        entityMgr.updateStatus(progress, status);
        progress = entityMgr.findProgressByRootOperationUid(progress.getRootOperationUID());
        response.setProperty(CollectionJobContext.PROGRESS_KEY, progress);
        return response;
    }

    protected String incrementalDataDirInHdfs(Progress progress) {
        Path incrementalDataDir = hdfsPathBuilder.constructRawDataFlowIncrementalDir(getSourceTableName(),
                progress.getStartDate(), progress.getEndDate());
        return incrementalDataDir.toString();
    }

    private String constructWhereClauseByDates(String timestampColumn, Date startDate, Date endDate) {
        return String.format("\"%s > '%s' AND %s <= '%s'\"", timestampColumn, dateFormat.format(startDate),
                timestampColumn, dateFormat.format(endDate));
    }

    private boolean cleanupHdfsDir(String targetDir, Progress progress) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                HdfsUtils.rmdir(yarnConfiguration, targetDir);
            }
        } catch (Exception e) {
            LoggingUtils.logError(log, progress, "Failed to cleanup hdfs dir " + targetDir, e);
            return false;
        }
        return true;
    }

    private boolean importIncrementalRawDataAndUpdateProgress(Progress progress) {
        String targetDir = incrementalDataDirInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStautsToFailed(progress, "Failed to cleanup HDFS path " + targetDir);
            return false;
        }

        String whereClause = constructWhereClauseByDates(getSrcTableTimestampColumn(),
                progress.getStartDate(), progress.getEndDate());
        String customer = getSqoopCustomerName(progress) + "-downloadRawData" ;

        if (!importSrcDBBySqoop(getSourceTableName(), targetDir, customer, whereClause, progress)) {
            updateStautsToFailed(progress, "Failed to import incremental data from DB.");
            return false;
        }

        long rowsDownloaded = jdbcTemplateDest.queryForObject("SELECT COUNT(*) FROM " + getSourceTableName()
                + " WHERE " + whereClause.substring(1, whereClause.lastIndexOf("\"")), Long.class);
        progress.setRowsDownloadedToHdfs(rowsDownloaded);

        String avscFile = getSourceTableName() + ".avsc";
        String schemaDir = hdfsPathBuilder.constructSchemaDir(getSourceTableName()).toString();
        schemaDir = schemaDir.endsWith("/") ? schemaDir : schemaDir + "/";
        String avscPath =  schemaDir + avscFile;
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
                HdfsUtils.rmdir(yarnConfiguration, avscPath);
            }
            String avroPath = targetDir + "/part-m-00000.avro";
            if (HdfsUtils.fileExists(yarnConfiguration, avroPath)) {
                Schema schema = AvroUtils.getSchema(yarnConfiguration, new org.apache.hadoop.fs.Path(avroPath));
                HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
            }
        } catch (Exception e) {
            LoggingUtils.logError(log, progress, "Failed to upload avsc.", e);
            updateStautsToFailed(progress, "Failed to upload avsc.");
            return false;
        }

        return true;
    }

    private boolean importSnapshotDestData(Progress progress) {
        String targetDir = hdfsPathBuilder.constructRawDataFlowSnapshotDir(getSourceTableName()).toString();
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStautsToFailed(progress, "Failed to cleanup HDFS path " + targetDir);
            return false;
        }

        String customer = getSqoopCustomerName(progress) + "-downloadSnapshotData" ;

        if (!importDestDBBySqoop(getDestTableName(), targetDir, customer, progress)) {
            updateStautsToFailed(progress, "Failed to import snapshot data from DB.");
            return false;
        }

        return true;
    }

    private boolean importSrcDBBySqoop(String table, String targetDir, String customer, String whereClause, Progress progress) {
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(srcHost).port(srcPort).db(srcDb).user(dbUser).password(dbPassword);
        DbCreds creds = new DbCreds(builder);
        try {
            sqoopService.importDataSyncWithWhereCondition(
                    table, targetDir, creds, assignedQueue, customer,
                    Collections.singletonList(getSrcTableSplitColumn()), "", whereClause, numMappers);
        } catch (Exception e) {
            LoggingUtils.logError(log, progress, "Failed to import data from source DB.", e);
            return false;
        }
        return true;
    }

    private boolean importDestDBBySqoop(String table, String targetDir, String customer, Progress progress) {
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(destHost).port(destPort).db(destDb).user(dbUser).password(dbPassword);
        DbCreds creds = new DbCreds(builder);
        try {
            sqoopService.importDataSync(table, targetDir, creds, assignedQueue, customer,
                    Collections.singletonList(getDestTableSplitColumn()), "", numMappers);
        } catch (Exception e) {
            LoggingUtils.logError(log, progress, "Failed to import data from destination DB.", e);
            return false;
        }
        return true;
    }

    private boolean transformRawDataToMostRecent(Progress progress) {
        // prepare target dir in hdfs
        String targetDir = workflowDirsInHdfs(progress);
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStautsToFailed(progress, "Failed to cleanup HDFS path " + targetDir);
            return false;
        }

        // execute data flow
        try {
            transformRawDataInternal(progress);
        } catch (Exception e) {
            log.error("Failed to transform raw data for progress " + progress, e);
            updateStautsToFailed(progress, "Failed to transform raw data.");
            return false;
        }

        return true;
    }

    void transformRawDataInternal(Progress progress) {
        collectionDataFlowService.executeTransformRawData(
                getSourceTableName(),
                incrementalDataDirInHdfs(progress),
                getMergeDataFlowQualifier()
        );
    }

    protected String workflowDirsInHdfs(Progress progress) {
        return hdfsPathBuilder.constructWorkFlowDir(getSourceTableName(),
                CollectionDataFlowKeys.MERGE_RAW_SNAPSHOT_FLOW).toString();
    }

    private String getSqoopCustomerName(Progress progress) {
        return entityMgr.getProgressClass().getSimpleName() + "[" + progress.getRootOperationUID() + "]";
    }

    private void dropJdbcTableIfExists(JdbcTemplate jdbcTemplate, String tableName) {
        jdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) DROP TABLE " + tableName);
    }


    private void updateStautsToFailed(Progress progress, String errorMsg) {
        progress.setStatusBeforeFailed(progress.getStatus());
        progress.setErrorMessage(errorMsg);
        entityMgr.updateStatus(progress, ArchiveProgressStatus.FAILED);
    }

}
