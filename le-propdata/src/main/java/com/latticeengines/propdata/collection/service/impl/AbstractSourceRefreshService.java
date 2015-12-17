package com.latticeengines.propdata.collection.service.impl;

import java.io.File;
import java.util.Collections;
import java.util.List;

import javax.annotation.PostConstruct;

import org.apache.avro.Schema;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.dataplatform.exposed.service.SqoopSyncJobService;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.propdata.collection.source.CollectionSource;
import com.latticeengines.propdata.collection.util.LoggingUtils;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class AbstractSourceRefreshService {

    private Log log;
    private ArchiveProgressEntityMgr entityMgr;
    private CollectionSource source;

    abstract ArchiveProgressEntityMgr getProgressEntityMgr();

    abstract Log getLogger();

    abstract CollectionSource getSource();

    @Autowired
    private SqoopSyncJobService sqoopService;

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

    protected boolean checkProgressStatus(ArchiveProgress progress,
                                        ArchiveProgressStatus expectedStatus,
                                          ArchiveProgressStatus inProgress) {
        if (progress == null) { return false; }

        if (inProgress.equals(progress.getStatus())) {
            return false;
        }

        if (ArchiveProgressStatus.FAILED.equals(progress.getStatus()) && (
                inProgress.equals(progress.getStatusBeforeFailed()) ||
                        expectedStatus.equals(progress.getStatusBeforeFailed())
        ) ) {
            return true;
        }

        if (!expectedStatus.equals(progress.getStatus())) {
            LoggingUtils.logError(log, progress, "Progress is not in the status " + expectedStatus + " but rather " +
                    progress.getStatus() + " before "
                    + inProgress + ".", new IllegalStateException());
            return false;
        }

        return true;
    }

    protected void logIfRetrying(ArchiveProgress progress) {
        if (progress.getStatus().equals(ArchiveProgressStatus.FAILED)) {
            int numRetries = progress.getNumRetries() + 1;
            progress.setNumRetries(numRetries);
            LoggingUtils.logInfo(log, progress, String.format("Retry [%d] from [%s].",
                    progress.getNumRetries(), progress.getStatusBeforeFailed()));
        }
    }


    protected String snapshotDirInHdfs() {
        return hdfsPathBuilder.constructRawDataFlowSnapshotDir(source).toString();
    }

    protected boolean cleanupHdfsDir(String targetDir, ArchiveProgress progress) {
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

    protected void extractSchema() throws Exception {
        String avscFile = source.getSourceName() + ".avsc";
        String schemaDir = hdfsPathBuilder.constructSchemaDir(source).toString();
        String avscPath =  schemaDir + "/" + avscFile;
        if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
            HdfsUtils.rmdir(yarnConfiguration, avscPath);
        }

        String avroDir = hdfsPathBuilder.constructRawDataFlowSnapshotDir(source).toString();
        List<String> files = HdfsUtils.getFilesByGlob(yarnConfiguration, avroDir + "/*.avro");
        if (files.size() > 0) {
            String avroPath = files.get(0);
            if (HdfsUtils.fileExists(yarnConfiguration, avroPath)) {
                Schema schema = AvroUtils.getSchema(yarnConfiguration, new org.apache.hadoop.fs.Path(avroPath));
                HdfsUtils.writeToFile(yarnConfiguration, avscPath, schema.toString());
            }
        } else {
            throw new IllegalStateException("No avro file found at " + avroDir);
        }
    }

    protected boolean importCollectionDBBySqoop(String table, String targetDir, String customer, String splitColumn,
                                       String whereClause, ArchiveProgress progress) {
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        DbCreds.Builder builder = new DbCreds.Builder();
        builder.host(dbHost).port(dbPort).db(db).user(dbUser).password(dbPassword);
        DbCreds creds = new DbCreds(builder);
        try {
            if (StringUtils.isEmpty(whereClause)) {
                sqoopService.importDataSync(table, targetDir, creds, assignedQueue, customer,
                        Collections.singletonList(splitColumn), "", numMappers);
            } else {
                sqoopService.importDataSyncWithWhereCondition(
                        table, targetDir, creds, assignedQueue, customer,
                        Collections.singletonList(splitColumn), "", whereClause, numMappers);
            }
        } catch (Exception e) {
            LoggingUtils.logError(log, progress, "Failed to import data from source DB.", e);
            return false;
        }
        return true;
    }

    protected String mergeWorkflowDirInHdfs() {
        return hdfsPathBuilder.constructWorkFlowDir(source, CollectionDataFlowKeys.MERGE_RAW_SNAPSHOT_FLOW).toString();
    }

    protected String getSqoopCustomerName(ArchiveProgress progress) {
        return source.getSourceName() + "[" + progress.getRootOperationUID() + "]";
    }

    protected boolean uploadAvroToDestTable(ArchiveProgress progress,
                                          String avroDir,
                                          String destTable,
                                          String indexCreationSql
                                          ) {
        String stageTableName = destTable + "_stage";
        String bakTableName = destTable + "_bak";
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        String customer = getSqoopCustomerName(progress);

        try {
            LoggingUtils.logInfo(log, progress, "Create a clean stage table " + stageTableName);
            dropJdbcTableIfExists(stageTableName);
            jdbcTemplate.execute("SELECT TOP 0 * INTO " + stageTableName + " FROM " + destTable);

            jdbcTemplate.execute(indexCreationSql);

            long uploadStartTime = System.currentTimeMillis();
            DbCreds.Builder builder = new DbCreds.Builder();
            builder.host(dbHost).port(dbPort).db(db).user(dbUser).password(dbPassword);
            DbCreds creds = new DbCreds(builder);
            sqoopService.exportDataSync(stageTableName, avroDir, creds, assignedQueue,
                    customer + "-upload-" + destTable, numMappers, null);
            long rowsUploaded = jdbcTemplate.queryForObject("SELECT COUNT(*) FROM " + stageTableName, Long.class);
            progress.setRowsUploadedToSql(rowsUploaded);
            LoggingUtils.logInfoWithDuration(log, progress, "Uploaded " + rowsUploaded + " rows to " + stageTableName, uploadStartTime);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to upload " + destTable + " to DB.", e);
            return false;
        } finally {
            FileUtils.deleteQuietly(new File(stageTableName+".java"));
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
        jdbcTemplate.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) DROP TABLE " + tableName);
    }

    private void swapTableNamesInDestDB(ArchiveProgress progress, String srcTable, String destTable) {
        dropJdbcTableIfExists(destTable);
        jdbcTemplate.execute("EXEC sp_rename '" + srcTable + "', '" + destTable + "'");
        LoggingUtils.logInfo(log, progress, String.format("Rename %s to %s.", srcTable, destTable));
    }

    protected void updateStatusToFailed(ArchiveProgress progress, String errorMsg, Exception e) {
        LoggingUtils.logError(log, progress, errorMsg, e);
        progress.setStatusBeforeFailed(progress.getStatus());
        progress.setErrorMessage(errorMsg);
        entityMgr.updateStatus(progress, ArchiveProgressStatus.FAILED);
    }

    protected String getDestTableName() { return source.getTableName(); }

}
