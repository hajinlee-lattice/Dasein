package com.latticeengines.cdl.workflow.steps.migrate;

import com.latticeengines.domain.exposed.serviceflows.cdl.steps.migrate.BaseMigrateImportStepConfiguration;
import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.TransformationWorkflowConfiguration;
import com.latticeengines.serviceflows.workflow.etl.BaseTransformWrapperStep;

public abstract class BaseMigrateImports<T extends BaseMigrateImportStepConfiguration> extends BaseTransformWrapperStep<T> {

    @Override
    protected TransformationWorkflowConfiguration executePreTransformation() {
        return null;
    }

    @Override
    protected void onPostTransformationCompleted() {

    }
}
