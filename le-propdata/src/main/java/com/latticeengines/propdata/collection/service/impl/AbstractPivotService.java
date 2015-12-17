package com.latticeengines.propdata.collection.service.impl;

import java.util.Date;

import javax.annotation.PostConstruct;

import org.apache.commons.logging.Log;

import com.latticeengines.common.exposed.util.HdfsUtils;
import com.latticeengines.domain.exposed.propdata.collection.ArchiveProgress;
import com.latticeengines.domain.exposed.propdata.collection.PivotProgress;
import com.latticeengines.domain.exposed.propdata.collection.ProgressStatus;
import com.latticeengines.propdata.collection.entitymanager.ArchiveProgressEntityMgr;
import com.latticeengines.propdata.collection.entitymanager.PivotProgressEntityMgr;
import com.latticeengines.propdata.collection.service.CollectionDataFlowKeys;
import com.latticeengines.propdata.collection.service.PivotService;
import com.latticeengines.propdata.collection.source.PivotedSource;
import com.latticeengines.propdata.collection.util.LoggingUtils;

public abstract class AbstractPivotService
        extends AbstractSourceRefreshService<PivotProgress> implements PivotService {

    private Log log;
    private PivotProgressEntityMgr entityMgr;
    private PivotedSource source;

    abstract PivotProgressEntityMgr getProgressEntityMgr();

    abstract ArchiveProgressEntityMgr getBaseSourceArchiveProgressEntityMgr();

    abstract PivotedSource getSource();

    abstract String getPivotDataFlowQualifier();

    abstract String createIndexForStageTableSql();

    @PostConstruct
    private void setEntityMgrs() {
        source = getSource();
        entityMgr = getProgressEntityMgr();
        log = getLogger();
    }

    @Override
    public PivotProgress startNewProgress(Date pivotDate, String creator) {
        PivotProgress progress = entityMgr.insertNewProgress(source, pivotDate, creator);
        LoggingUtils.logInfo(log, progress, "Started a new progress with pivotDate=" + pivotDate);
        return progress;
    }


    @Override
    public PivotProgress pivot(PivotProgress progress) {
        // check request context
        if (!checkProgressStatus(progress, ProgressStatus.NEW, ProgressStatus.PIVOTING)) {
            return progress;
        }

        // update status
        long startTime = System.currentTimeMillis();
        logIfRetrying(progress);
        entityMgr.updateStatus(progress, ProgressStatus.PIVOTING);
        LoggingUtils.logInfo(log, progress, "Start transforming ...");

        // merge raw and snapshot, then output most recent records
        if (!pivotInternal(progress)) {
            return progress;
        }

        LoggingUtils.logInfoWithDuration(log, progress, "Transformed.", startTime);
        progress.setNumRetries(0);
        return entityMgr.updateStatus(progress, ProgressStatus.PIVOTED);
    }


    @Override
    public PivotProgress exportToDB(PivotProgress progress) {
        // check request context
//        if (!checkProgressStatus(progress, ProgressStatus.PIVOTED, ProgressStatus.UPLOADING)) {
//            return progress;
//        }

        // update status
        long startTime = System.currentTimeMillis();
        logIfRetrying(progress);
        entityMgr.updateStatus(progress, ProgressStatus.UPLOADING);
        LoggingUtils.logInfo(log, progress, "Start uploading ...");

        // upload source
        long uploadStartTime = System.currentTimeMillis();
        String sourceDir = snapshotDirInHdfs();
        String destTable = source.getTableName();
        System.out.println(sourceDir);
        if (!uploadAvroToCollectionDB(progress, sourceDir, destTable, createIndexForStageTableSql())) {
            return progress;
        }

        long rowsUploaded = jdbcTemplateCollectionDB.queryForObject("SELECT COUNT(*) FROM " + destTable, Long.class);
        progress.setRowsGenerated(rowsUploaded);
        LoggingUtils.logInfoWithDuration(getLogger(),
                progress, "Uploaded " + rowsUploaded + " rows to " + destTable, uploadStartTime);

        // finish
        LoggingUtils.logInfoWithDuration(log, progress, "Uploaded.", startTime);
        progress.setNumRetries(0);
        return entityMgr.updateStatus(progress, ProgressStatus.UPLOADED);
    }

    @Override
    public ArchiveProgress findRunningJobOnBaseSource() {
        return getBaseSourceArchiveProgressEntityMgr().findProgressNotInFinalState(source.getBaseSource());
    }

    private boolean pivotInternal(PivotProgress progress) {
        String targetDir = pivotWorkflowDirInHdfs();
        if (!cleanupHdfsDir(targetDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + targetDir, null);
            return false;
        }
        try {
            collectionDataFlowService.executePivotSnapshotData(
                    source,
                    baseSourceDirInHdfs(),
                    getPivotDataFlowQualifier()
            );
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform raw data.", e);
            return false;
        }

        // copy result to snapshot
        try {
            String snapshotDir = snapshotDirInHdfs();
            String srcDir = pivotWorkflowDirInHdfs() + "/Output";
            HdfsUtils.rmdir(yarnConfiguration, snapshotDir);
            HdfsUtils.copyFiles(yarnConfiguration, srcDir, snapshotDir);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to copy pivoted data to Snapshot folder.", e);
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

    private String pivotWorkflowDirInHdfs() {
        return hdfsPathBuilder.constructWorkFlowDir(getSource(), CollectionDataFlowKeys.PIVOT_SNAPSHOT_FLOW).toString();
    }

    private String baseSourceDirInHdfs() {
        return hdfsPathBuilder.constructRawDataFlowSnapshotDir(getSource().getBaseSource()).toString();
    }

}
