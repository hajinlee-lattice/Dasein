package com.latticeengines.propdata.engine.transformation.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;

import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.propdata.core.source.DerivedSource;
import com.latticeengines.propdata.core.source.Source;
import com.latticeengines.propdata.engine.transformation.configuration.TransformationConfiguration;

/**
 * This is the base implementation of the transformatin service
 * for simpliest sources: single base source, single dataflow.
 */
public abstract class SimpleTransformationServiceBase<T extends TransformationConfiguration, P extends TransformationFlowParameters>
        extends AbstractTransformationService<T> {

    @Autowired
    protected SimpleTransformationDataFlowService dataFlowService;

    protected abstract String getDataFlowBeanName();

    protected abstract P getDataFlowParameters(TransformationProgress progress, T transformationConfiguration);

    @Override
    public boolean isManualTriggerred() {
        return true;
    }

    protected TransformationProgress transformHook(TransformationProgress progress, T transformationConfiguration) {
        String workflowDir = initialDataFlowDirInHdfs(progress);
        if (!cleanupHdfsDir(workflowDir, progress)) {
            updateStatusToFailed(progress, "Failed to cleanup HDFS path " + workflowDir, null);
            return null;
        }
        try {
            Map<Source, String> baseSourceVersionMap = new HashMap<>();
            Source baseSource = ((DerivedSource) getSource()).getBaseSources()[0];
            String baseSourceVersion = progress.getBaseSourceVersions();
            if (org.apache.commons.lang.StringUtils.isEmpty(baseSourceVersion)) {
                baseSourceVersion = hdfsSourceEntityMgr.getCurrentVersion(baseSource);
            }
            baseSourceVersionMap.put(baseSource, baseSourceVersion);

            P parameters = getDataFlowParameters(progress, transformationConfiguration);

            dataFlowService.executeDataFlow(getSource(), workflowDir, baseSourceVersionMap, getDataFlowBeanName(),
                    parameters);
        } catch (Exception e) {
            updateStatusToFailed(progress, "Failed to transform data.", e);
            return null;
        }

        if (doPostProcessing(progress, workflowDir)) {
            return progress;
        } else {
            return null;
        }
    }

}
