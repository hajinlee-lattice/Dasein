package com.latticeengines.datacloud.collection.service.impl;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.slf4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.yarn.client.YarnClient;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.CipherUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.datacloud.collection.entitymgr.ProgressEntityMgr;
import com.latticeengines.datacloud.collection.service.CollectionDataFlowService;
import com.latticeengines.datacloud.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.datacloud.core.source.CollectedSource;
import com.latticeengines.datacloud.core.source.Source;
import com.latticeengines.datacloud.core.util.HdfsPathBuilder;
import com.latticeengines.datacloud.core.util.LoggingUtils;
import com.latticeengines.datacloud.etl.entitymgr.SourceColumnEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.datacloud.manage.Progress;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.dataplatform.SqoopExporter;
import com.latticeengines.domain.exposed.dataplatform.SqoopImporter;
import com.latticeengines.domain.exposed.modeling.DbCreds;
import com.latticeengines.proxy.exposed.sqoop.SqoopProxy;

public abstract class SourceRefreshServiceBase<P extends Progress> {

    private static final String SQOOP_OPTION_WHERE = "--where";

    private static final String SQOOP_OPTION_BATCH = "--batch";

    private static final String JVM_PARAM_EXPORT_STATEMENTS_PER_TRANSACTION = "-Dexport.statements.per.transaction=1";

    private static final String JVM_PARAM_EXPORT_RECORDS_PER_STATEMENT = "-Dsqoop.export.records.per.statement=1000";

    private static final String SQOOP_CUSTOMER_PROPDATA = "PropData-";

    abstract ProgressEntityMgr<P> getProgressEntityMgr();

    abstract Logger getLogger();

    abstract Source getSource();

    @Autowired
    protected SqoopProxy sqoopProxy;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected YarnClient yarnClient;

    @Autowired
    protected CollectionDataFlowService collectionDataFlowService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplateCollectionDB;

    @Autowired
    @Qualifier(value = "propDataBulkJdbcTemplate")
    protected JdbcTemplate jdbcTemplateBulkDB;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.collection.host}")
    private String dbHost;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.collection.port}")
    private int dbPort;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.collection.db}")
    private String db;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.user}")
    private String dbUser;

    // DataCloud CollectionDB is shutdown
    @Deprecated
    @Value("${datacloud.password.encrypted}")
    private String dbPassword;

    @Value("${datacloud.collection.sqoop.mapper.number}")
    private int numMappers;

    public P findRunningJob() {
        return getProgressEntityMgr().findRunningProgress(getSource());
    }

    public P findJobToRetry() {
        return getProgressEntityMgr().findEarliestFailureUnderMaxRetry(getSource());
    }

    protected boolean checkProgressStatus(P progress, ProgressStatus expectedStatus, ProgressStatus inProgress) {
        if (progress == null) {
            return false;
        }

        if (inProgress.equals(progress.getStatus())) {
            return false;
        }

        if (ProgressStatus.FAILED.equals(progress.getStatus()) && (inProgress.equals(progress.getStatusBeforeFailed())
                || expectedStatus.equals(progress.getStatusBeforeFailed()))) {
            return true;
        }

        if (!expectedStatus.equals(progress.getStatus())) {
            LoggingUtils
                    .logError(getLogger(),
                            progress, "Progress is not in the status " + expectedStatus + " but rather "
                                    + progress.getStatus() + " before " + inProgress + ".",
                            new IllegalStateException());
            return false;
        }

        return true;
    }

    protected void logIfRetrying(P progress) {
        if (progress.getStatus().equals(ProgressStatus.FAILED)) {
            int numRetries = progress.getNumRetries() + 1;
            progress.setNumRetries(numRetries);
            LoggingUtils.logInfo(getLogger(), progress,
                    String.format("Retry [%d] from [%s].", progress.getNumRetries(), progress.getStatusBeforeFailed()));
        }
    }

    protected String snapshotDirInHdfs(P progress) {
        return hdfsPathBuilder.constructTransformationSourceDir(getSource(), getVersionString(progress)).toString();
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

    public String getVersionString(P progress) {
        return HdfsPathBuilder.dateFormat.format(progress.getCreateTime());
    }

    protected void extractSchema(P progress) throws Exception {
        String version = getVersionString(progress);
        String avscPath = hdfsPathBuilder.constructSchemaFile(getSource().getSourceName(), version).toString();
        if (HdfsUtils.fileExists(yarnConfiguration, avscPath)) {
            HdfsUtils.rmdir(yarnConfiguration, avscPath);
        }

        String avroDir = hdfsPathBuilder.constructSnapshotDir(getSource().getSourceName(), getVersionString(progress))
                .toString();
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

    protected boolean importFromCollectionDB(String table, String targetDir, String splitColumn, String whereClause,
            P progress) {
        try {
            SqoopImporter importer = getCollectionDbImporter(table, targetDir, splitColumn, whereClause);
            ApplicationId appId = ConverterUtils
                    .toApplicationId(sqoopProxy.importData(importer).getApplicationIds().get(0));
            FinalApplicationStatus status = YarnUtils.waitFinalStatusForAppId(yarnClient, appId, 24 * 3600);
            if (!FinalApplicationStatus.SUCCEEDED.equals(status)) {
                throw new IllegalStateException("The final state of " + appId + " is not "
                        + FinalApplicationStatus.SUCCEEDED + " but rather " + status);
            }

        } catch (Exception e) {
            LoggingUtils.logError(getLogger(), progress, "Failed to import data from source DB.", e);
            return false;
        }
        return true;
    }

    protected void updateStatusToFailed(P progress, String errorMsg, Exception e) {
        LoggingUtils.logError(getLogger(), progress, errorMsg, e);
        progress.setStatusBeforeFailed(progress.getStatus());
        progress.setErrorMessage(errorMsg);
        getProgressEntityMgr().updateStatus(progress, ProgressStatus.FAILED);
    }

    protected P finishProgress(P progress) {
        progress.setNumRetries(0);
        LoggingUtils.logInfo(getLogger(), progress, "Finished.");
        return getProgressEntityMgr().updateStatus(progress, ProgressStatus.FINISHED);
    }

    protected Long countSourceTable(P progress) {
        Long startTime = System.currentTimeMillis();
        Source source = getSource();
        String version = getVersionString(progress);
        if ((progress instanceof ArchiveProgress) && (source instanceof CollectedSource)) {
            ArchiveProgress archiveProgress = (ArchiveProgress) progress;
            version = HdfsPathBuilder.dateFormat.format(archiveProgress.getEndDate());
        }
        Long count = hdfsSourceEntityMgr.count(source, version);
        LoggingUtils.logInfoWithDuration(getLogger(), progress,
                String.format("There are %d rows in " + getSource().getSourceName(), count), startTime);
        return count;
    }

    // DataCloud CollectionDB is shutdown
    @Deprecated
    protected SqoopExporter getCollectionDbExporter(String sqlTable, String avroDir) {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.host(dbHost).port(dbPort).db(db).user(dbUser).encryptedPassword(CipherUtils.encrypt(dbPassword));

        return new SqoopExporter.Builder().setCustomer(SQOOP_CUSTOMER_PROPDATA + sqlTable).setNumMappers(numMappers)
                .setTable(sqlTable).setSourceDir(avroDir).setDbCreds(new DbCreds(credsBuilder))
                .addHadoopArg(JVM_PARAM_EXPORT_RECORDS_PER_STATEMENT)
                .addHadoopArg(JVM_PARAM_EXPORT_STATEMENTS_PER_TRANSACTION).addExtraOption(SQOOP_OPTION_BATCH)
                .setSync(false).build();
    }

    // DataCloud CollectionDB is shutdown
    @Deprecated
    protected SqoopImporter getCollectionDbImporter(String sqlTable, String avroDir, String splitColumn,
            String whereClause) {
        DbCreds.Builder credsBuilder = new DbCreds.Builder();
        credsBuilder.host(dbHost).port(dbPort).db(db).user(dbUser).encryptedPassword(CipherUtils.encrypt(dbPassword));

        SqoopImporter.Builder builder = new SqoopImporter.Builder().setCustomer(SQOOP_CUSTOMER_PROPDATA + sqlTable)
                .setNumMappers(numMappers).setSplitColumn(splitColumn).setTable(sqlTable).setTargetDir(avroDir)
                .setDbCreds(new DbCreds(credsBuilder)).setSync(false);

        if (StringUtils.isNotEmpty(whereClause)) {
            builder = builder.addExtraOption(SQOOP_OPTION_WHERE).addExtraOption(whereClause);
        }

        return builder.build();
    }

}
