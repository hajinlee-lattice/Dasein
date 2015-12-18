package com.latticeengines.propdata.collection.service.impl;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.List;
import java.util.TimeZone;

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
import com.latticeengines.domain.exposed.propdata.collection.Progress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.HdfsSourceEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.ProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.propdata.collection.source.Source;
import com.latticeengines.propdata.collection.util.LoggingUtils;
import com.latticeengines.scheduler.exposed.LedpQueueAssigner;

public abstract class AbstractSourceRefreshService<P extends Progress> {

    abstract ProgressEntityMgr<P> getProgressEntityMgr();

    abstract Log getLogger();

    abstract Source getSource();

    @Autowired
    private SqoopSyncJobService sqoopService;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected CollectionDataFlowService collectionDataFlowService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplateCollectionDB;

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

    protected boolean checkProgressStatus(P progress, ProgressStatus expectedStatus, ProgressStatus inProgress) {
        if (progress == null) { return false; }

        if (inProgress.equals(progress.getStatus())) {
            return false;
        }

        if (ProgressStatus.FAILED.equals(progress.getStatus()) && (
                inProgress.equals(progress.getStatusBeforeFailed()) ||
                        expectedStatus.equals(progress.getStatusBeforeFailed())
        ) ) {
            return true;
        }

        if (!expectedStatus.equals(progress.getStatus())) {
            LoggingUtils.logError(getLogger(),
                    progress, "Progress is not in the status " + expectedStatus + " but rather " +
                    progress.getStatus() + " before "
                    + inProgress + ".", new IllegalStateException());
            return false;
        }

        return true;
    }

    protected void logIfRetrying(P progress) {
        if (progress.getStatus().equals(ProgressStatus.FAILED)) {
            int numRetries = progress.getNumRetries() + 1;
            progress.setNumRetries(numRetries);
            LoggingUtils.logInfo(getLogger(), progress, String.format("Retry [%d] from [%s].",
                    progress.getNumRetries(), progress.getStatusBeforeFailed()));
        }
    }


    protected String snapshotDirInHdfs(P progress) {
        return hdfsPathBuilder.constructSnapshotDir(getSource(), getVersionString(progress)).toString();
    }

    protected boolean cleanupHdfsDir(String targetDir, P progress) {
        try {
            if (HdfsUtils.fileExists(yarnConfiguration, targetDir)) {
                HdfsUtils.rmdir(yarnConfiguration, targetDir);
            }
        } catch (Exception e) {
            LoggingUtils.logError(getLogger(), progress, "Failed to cleanup hdfs dir " + targetDir, e);
            return false;
        }
        return true;
    }

    protected String getVersionString(P progress) {
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd_HH-mm-ss_z");
        dateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
        return dateFormat.format(progress.getCreateTime());
    }

    protected void extractSchema(P progress) throws Exception {
        String version = getVersionString(progress);
        String avscPath =  hdfsPathBuilder.constructSchemaFile(getSource(), version).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
            HdfsUtils.rmdir(yarnConfiguration, avscPath);
        }

        String avroDir = hdfsPathBuilder.constructSnapshotDir(getSource(), getVersionString(progress)).toString();
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

    protected boolean importFromCollectionDB(String table, String targetDir, String customer, String splitColumn,
                                             String whereClause, P progress) {
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
            LoggingUtils.logError(getLogger(), progress, "Failed to import data from source DB.", e);
            return false;
        }
        return true;
    }

    protected String getSqoopCustomerName(P progress) {
        return getSource().getSourceName() + "[" + progress.getRootOperationUID() + "]";
    }

    protected boolean uploadAvroToCollectionDB(P progress,
                                               String avroDir,
                                               String destTable,
                                               String indexCreationSql
                                          ) {
        String stageTableName = destTable + "_stage";
        String bakTableName = destTable + "_bak";
        String assignedQueue = LedpQueueAssigner.getPropDataQueueNameForSubmission();
        String customer = getSqoopCustomerName(progress);

        try {
            LoggingUtils.logInfo(getLogger(), progress, "Create a clean stage table " + stageTableName);
            dropJdbcTableIfExists(stageTableName);
            jdbcTemplateCollectionDB.execute("SELECT TOP 0 * INTO " + stageTableName + " FROM " + destTable);

            jdbcTemplateCollectionDB.execute(indexCreationSql);

            DbCreds.Builder builder = new DbCreds.Builder();
            builder.host(dbHost).port(dbPort).db(db).user(dbUser).password(dbPassword);
            DbCreds creds = new DbCreds(builder);
            sqoopService.exportDataSync(stageTableName, avroDir, creds, assignedQueue,
                    customer + "-upload-" + destTable, numMappers, null);
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
        jdbcTemplateCollectionDB.execute("IF EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'"
                + tableName + "') AND type in (N'U')) DROP TABLE " + tableName);
    }

    private void swapTableNamesInDestDB(P progress, String srcTable, String destTable) {
        dropJdbcTableIfExists(destTable);
        jdbcTemplateCollectionDB.execute("EXEC sp_rename '" + srcTable + "', '" + destTable + "'");
        LoggingUtils.logInfo(getLogger(), progress, String.format("Rename %s to %s.", srcTable, destTable));
    }

    protected void updateStatusToFailed(P progress, String errorMsg, Exception e) {
        LoggingUtils.logError(getLogger(), progress, errorMsg, e);
        progress.setStatusBeforeFailed(progress.getStatus());
        progress.setErrorMessage(errorMsg);
        getProgressEntityMgr().updateStatus(progress, ProgressStatus.FAILED);
    }

    protected String getDestTableName() { return getSource().getTableName(); }

}
