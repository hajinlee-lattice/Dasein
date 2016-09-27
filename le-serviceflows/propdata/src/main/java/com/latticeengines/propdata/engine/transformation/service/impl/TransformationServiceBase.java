package com.latticeengines.propdata.engine.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.datacloud.dataflow.CollectionDataFlowKeys;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.HdfsHelper;
import com.latticeengines.propdata.engine.transformation.ProgressHelper;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;

public abstract class TransformationServiceBase {

    @Autowired
    private HdfsHelper hdfsHelper;

    @Autowired
    private ProgressHelper progressHelper;

    abstract TransformationProgressEntityMgr getProgressEntityMgr();

    abstract Log getLogger();

    abstract Source getSource();

    public TransformationProgress findRunningJob() {
        return progressHelper.findRunningJob(getProgressEntityMgr(), getSource());
    }

    public TransformationProgress findJobToRetry(String version) {
        return progressHelper.findJobToRetry(getProgressEntityMgr(), getSource(), version);
    }

    protected boolean checkProgressStatus(TransformationProgress progress) {
        return progressHelper.checkProgressStatus(progress, getLogger());
    }

    protected void logIfRetrying(TransformationProgress progress) {
        progressHelper.logIfRetrying(progress, getLogger());
    }

    protected String snapshotDirInHdfs(TransformationProgress progress) {
        return hdfsHelper.snapshotDirInHdfs(progress, getSource());
    }

    protected boolean cleanupHdfsDir(String targetDir, TransformationProgress progress) {
        return hdfsHelper.cleanupHdfsDir(targetDir, progress, getLogger());
    }

    public String getVersionString(TransformationProgress progress) {
        return hdfsHelper.getVersionString(progress);
    }

    protected void extractSchema(TransformationProgress progress, String avroDir) throws Exception {
        hdfsHelper.extractSchema(progress, getSource(), avroDir, getVersion(progress));
    }

    public void updateStatusToFailed(TransformationProgress progress, String errorMsg, Exception e) {
        progressHelper.updateStatusToFailed(getProgressEntityMgr(), progress, errorMsg, e, getLogger());
        ;
    }

    protected TransformationProgress finishProgress(TransformationProgress progress) {
        return progressHelper.finishProgress(getProgressEntityMgr(), progress, getLogger());
    }

    protected String sourceDirInHdfs(Source source) {
        String sourceDirInHdfs = getHdfsPathBuilder().constructTransformationSourceDir(source).toString();
        getLogger().info("sourceDirInHdfs for " + getSource().getSourceName() + " = " + sourceDirInHdfs);
        return sourceDirInHdfs;
    }

    protected String sourceVersionDirInHdfs(TransformationProgress progress) {
        String sourceDirInHdfs = getHdfsPathBuilder()
                .constructTransformationSourceDir(getSource(), progress.getVersion()).toString();
        getLogger().info("sourceVersionDirInHdfs for " + getSource().getSourceName() + " = " + sourceDirInHdfs);
        return sourceDirInHdfs;
    }

    protected String initialDataFlowDirInHdfs(TransformationProgress progress) {
        String workflowDir = dataFlowDirInHdfs(progress, CollectionDataFlowKeys.TRANSFORM_FLOW);
        getLogger().info("initialDataFlowDirInHdfs for " + getSource().getSourceName() + " = " + workflowDir);
        return workflowDir;
    }

    protected String dataFlowDirInHdfs(TransformationProgress progress, String dataFlowName) {
        String dataflowDir = getHdfsPathBuilder().constructWorkFlowDir(getSource(), dataFlowName)
                .append(progress.getRootOperationUID()).toString();
        getLogger().info("dataFlowDirInHdfs for " + getSource().getSourceName() + " = " + dataflowDir);
        return dataflowDir;
    }

    protected String finalWorkflowOuputDir(TransformationProgress progress) {
        // Firehose transformation has special setting. Otherwise, it is the
        // default dataFlowDir
        return initialDataFlowDirInHdfs(progress);
    }

    protected Configuration getYarnConfiguration() {
        return hdfsHelper.getYarnConfiguration();
    }

    protected HdfsPathBuilder getHdfsPathBuilder() {
        return hdfsHelper.getHdfsPathBuilder();
    }

    protected String getVersion(TransformationProgress progress) {
        return progress.getVersion();
    }
}
