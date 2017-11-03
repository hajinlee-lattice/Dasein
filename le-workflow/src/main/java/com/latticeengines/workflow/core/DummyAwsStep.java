package com.latticeengines.workflow.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSBatchConfiguration;

@Component("dummyAwsStep")
public class DummyAwsStep extends BaseAwsBatchStep<AWSBatchConfiguration> {

    private static final Logger log = LoggerFactory.getLogger(DummyAwsStep.class);

    @Override
    public void executeInline() {
        AWSBatchConfiguration config = getConfiguration();
        log.info("Inside DummyAwsStep." + " Config=" + config);
    }
}
