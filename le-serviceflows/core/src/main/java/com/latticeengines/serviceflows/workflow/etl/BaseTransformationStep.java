package com.latticeengines.serviceflows.workflow.etl;

import org.springframework.beans.factory.annotation.Autowired;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.TransformerConfig;
import com.latticeengines.domain.exposed.workflow.BaseStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;
import com.latticeengines.proxy.exposed.matchapi.ColumnMetadataProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

public abstract class BaseTransformationStep<T extends BaseStepConfiguration> extends BaseWorkflowStep<T> {

    private static int MAX_LOOPS = 1800;
    private static final ObjectMapper OM = new ObjectMapper();
    
    @Autowired
    protected TransformationProxy transformationProxy;

    @Autowired
    private ColumnMetadataProxy columnMetadataProxy;

    protected String getDataCloudVersion() {
        return columnMetadataProxy.latestVersion("").getVersion();
    }

    protected String useEngine(TransformerConfig conf, String engine) {
        TransformationFlowParameters.EngineConfiguration engineConf = new TransformationFlowParameters.EngineConfiguration();
        engineConf.setEngine(engine);
        return appendEngineConf(conf, engineConf);
    }

    private String appendEngineConf(TransformerConfig conf, TransformationFlowParameters.EngineConfiguration engineConf) {
        ObjectNode on = OM.valueToTree(conf);
        on.set("EngineConfig", OM.valueToTree(engineConf));
        return JsonUtils.serialize(on);
    }

    protected void waitForFinish(TransformationProgress progress) {
        TransformationProgress progressInDb = null;
        for (int i = 0; i < MAX_LOOPS; i++) {
            progressInDb = transformationProxy.getProgress(progress.getRootOperationUID());
            if (ProgressStatus.FINISHED.equals(progressInDb.getStatus())
                    || ProgressStatus.FAILED.equals(progressInDb.getStatus())) {
                break;
            }
            log.info("TransformationProgress Id=" + progressInDb.getPid() + " status=" + progressInDb.getStatus());
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                log.warn("Waiting was interrupted, message=" + e.getMessage());
            }
        }

        if (ProgressStatus.FINISHED.equals(progressInDb.getStatus())) {
            log.info("Consolidate data pipeline is finished");
        } else if (ProgressStatus.FAILED.equals(progressInDb.getStatus())) {
            String error = "Consolidate data pipeline failed!";
            log.error(error);
            throw new RuntimeException(error + " Error=" + progressInDb.getErrorMessage());
        } else {
            String error = "Consolidate data pipeline timeout!";
            log.error(error);
            throw new RuntimeException(error);
        }
    }
}
