package com.latticeengines.propdata.engine.transformation.service.impl;

import org.apache.commons.logging.Log;
import org.apache.hadoop.conf.Configuration;
import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.propdata.manage.TransformationProgress;
import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.HdfsHelper;
import com.latticeengines.propdata.engine.transformation.ProgressHelper;
import com.latticeengines.propdata.engine.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.propdata.engine.transformation.service.TransformationDataFlowService;

public abstract class TransformationServiceBase {
    @Autowired
    private HdfsHelper hdfsHelper;

    @Autowired
    private ProgressHelper progressHelper;

    abstract TransformationProgressEntityMgr getProgressEntityMgr();

    abstract Log getLogger();

    abstract Source getSource();

    abstract TransformationDataFlowService getTransformationDataFlowService();

    public TransformationProgress findRunningJob() {
        return progressHelper.findRunningJob(getProgressEntityMgr(), getSource());
    }

    public TransformationProgress findJobToRetry() {
        return progressHelper.findJobToRetry(getProgressEntityMgr(), getSource());
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
