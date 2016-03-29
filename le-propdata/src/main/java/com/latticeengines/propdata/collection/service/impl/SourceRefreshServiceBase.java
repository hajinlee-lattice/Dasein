package com.latticeengines.propdata.collection.service.impl;

import java.util.List;

import org.apache.avro.Schema;
import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.util.ConverterUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.common.exposed.util.YarnUtils;
import com.latticeengines.domain.exposed.api.AppSubmission;
import com.latticeengines.domain.exposed.propdata.ImportRequest;
import com.latticeengines.domain.exposed.propdata.manage.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.manage.Progress;
import com.latticeengines.domain.exposed.propdata.manage.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.SourceColumnEntityMgr;
import com.latticeengines.propdata.collection.service.CollectionDataFlowService;
import com.latticeengines.propdata.core.datasource.DataSourceService;
import com.latticeengines.propdata.core.entitymgr.HdfsSourceEntityMgr;
import com.latticeengines.propdata.core.service.SqlService;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.CollectedSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.core.util.LoggingUtils;
import com.latticeengines.proxy.exposed.propdata.SqlProxy;

public abstract class SourceRefreshServiceBase<P extends Progress> {

    abstract ProgressEntityMgr<P> getProgressEntityMgr();

    abstract Log getLogger();

    abstract Source getSource();

    @Autowired
    protected SqlService sqlService;

    @Autowired
    protected SqlProxy sqlProxy;

    @Autowired
    protected HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    protected Configuration yarnConfiguration;

    @Autowired
    protected CollectionDataFlowService collectionDataFlowService;

    @Autowired
    protected HdfsSourceEntityMgr hdfsSourceEntityMgr;

    @Autowired
    protected SourceColumnEntityMgr sourceColumnEntityMgr;

    @Autowired
    protected DataSourceService dataSourceService;

    @Autowired
    @Qualifier(value = "propDataCollectionJdbcTemplate")
    protected JdbcTemplate jdbcTemplateCollectionDB;

    @Autowired
    @Qualifier(value = "propDataBulkJdbcTemplate")
    protected JdbcTemplate jdbcTemplateBulkDB;

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

    public String getVersionString(P progress) {
        return HdfsPathBuilder.dateFormat.format(progress.getCreateTime());
    }

    protected void extractSchema(P progress) throws Exception {
        String version = getVersionString(progress);
        String avscPath = hdfsPathBuilder.constructSchemaFile(getSource(), version).toString();
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

    protected boolean importFromCollectionDB(String table, String targetDir, String splitColumn, String whereClause,
            P progress) {
        try {
            ImportRequest importRequest = new ImportRequest();
            importRequest.setSqlTable(table);
            importRequest.setAvroDir(targetDir);
            importRequest.setSplitColumn(splitColumn);
            importRequest.setWhereClause(whereClause);
            AppSubmission submission = sqlProxy.importTable(importRequest);
            ApplicationId appId = ConverterUtils.toApplicationId(submission.getApplicationIds().get(0));
            FinalApplicationStatus status =
                    YarnUtils.waitFinalStatusForAppId(yarnConfiguration, appId, 24 * 3600 * 1000L);
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

}
