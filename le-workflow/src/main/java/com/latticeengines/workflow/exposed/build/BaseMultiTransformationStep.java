package com.latticeengines.workflow.exposed.build;

import javax.inject.Inject;

import com.latticeengines.common.exposed.util.JsonUtils;
import com.latticeengines.domain.exposed.datacloud.manage.ProgressStatus;
import com.latticeengines.domain.exposed.datacloud.manage.TransformationProgress;
import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.workflow.BaseMultiTransformationStepConfiguration;
import com.latticeengines.proxy.exposed.datacloudapi.TransformationProxy;

public abstract class BaseMultiTransformationStep<T extends BaseMultiTransformationStepConfiguration> extends BaseWorkflowStep<T> {

    @Inject
    private TransformationProxy transformationProxy;

    @Override
    public void execute() {
        intializeConfiguration();
        int index = 0;
        TransformationProgress progress = null;
        do {
            PipelineTransformationRequest request = generateRequest(progress, index);
            progress = transformationProxy.transform(request, configuration.getPodId());
            log.info("progress: {}", progress);
            waitForProgressStatus(progress.getRootOperationUID());
        } while (iteratorContinued(progress, index++));
        onPostTransformationCompleted();
    }

    private ProgressStatus waitForProgressStatus(String rootId) {
        while (true) {
            TransformationProgress progress = transformationProxy.getProgress(rootId);
            log.info("progress is {}", JsonUtils.serialize(progress));
            if (progress == null) {
                throw new IllegalStateException(String.format("transformationProgress cannot be null, rootId is %s.",
                        rootId));
            }
            if (progress.getStatus().equals(ProgressStatus.FINISHED)) {
                return progress.getStatus();
            }
            if (progress.getStatus().equals(ProgressStatus.FAILED)) {
                throw new RuntimeException(
                        "Transformation failed, check log for detail.: " + JsonUtils.serialize(progress));
            }
            try {
                Thread.sleep(120000L);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    protected abstract void intializeConfiguration();

    protected abstract PipelineTransformationRequest generateRequest(TransformationProgress progress, int index);

    protected abstract boolean iteratorContinued(TransformationProgress progress, int index);

    protected abstract void onPostTransformationCompleted();
}
