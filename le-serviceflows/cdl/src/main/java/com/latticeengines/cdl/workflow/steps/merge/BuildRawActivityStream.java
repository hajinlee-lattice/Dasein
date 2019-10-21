package com.latticeengines.cdl.workflow.steps.merge;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.config.ConfigurableBeanFactory;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.datacloud.transformation.PipelineTransformationRequest;
import com.latticeengines.domain.exposed.serviceflows.cdl.steps.process.ProcessActivityStreamStepConfiguration;

@Component(BuildRawActivityStream.BEAN_NAME)
@Scope(ConfigurableBeanFactory.SCOPE_PROTOTYPE)
@Lazy
public class BuildRawActivityStream extends BaseMergeImports<ProcessActivityStreamStepConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(BuildRawActivityStream.class);

    static final String BEAN_NAME = "buildRawActivityStream";

    @Override
    protected void initializeConfiguration() {
        super.initializeConfiguration();
        // TODO
    }

    @Override
    protected void onPostTransformationCompleted() {
        // TODO
    }

    @Override
    protected PipelineTransformationRequest getConsolidateRequest() {
        // TODO
        return null;
    }
}
