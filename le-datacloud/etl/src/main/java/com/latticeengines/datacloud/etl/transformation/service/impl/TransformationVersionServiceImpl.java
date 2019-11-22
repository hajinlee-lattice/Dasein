package com.latticeengines.datacloud.etl.transformation.service.impl;

import javax.inject.Inject;

import org.springframework.stereotype.Component;

import com.latticeengines.datacloud.etl.service.DataCloudEngineVersionService;
import com.latticeengines.datacloud.etl.transformation.entitymgr.TransformationProgressEntityMgr;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngine;
import com.latticeengines.domain.exposed.datacloud.orchestration.DataCloudEngineStage;

@Component("transformationVersionService")
public class TransformationVersionServiceImpl implements DataCloudEngineVersionService {

    @Inject
    protected TransformationProgressEntityMgr progressEntityMgr;

    @Override
    public DataCloudEngineStage findProgressAtVersion(DataCloudEngineStage stage) {
        stage.setEngine(DataCloudEngine.TRANSFORMATION);
        TransformationProgress progress = progressEntityMgr.findPipelineProgressAtVersion(stage.getEngineName(),
                stage.getVersion());
        if (progress == null) {
            stage.setStatus(ProgressStatus.NOTSTARTED);
        } else {
            stage.setStatus(progress.getStatus());
            stage.setMessage(progress.getErrorMessage());
        }
        return stage;
    }

    @Override
    public DataCloudEngine getEngine() {
        return DataCloudEngine.TRANSFORMATION;
    }

    @Override
    public String findCurrentVersion(String pipelineName) {
        return progressEntityMgr.getLatestSuccessVersion(pipelineName);
    }
}
