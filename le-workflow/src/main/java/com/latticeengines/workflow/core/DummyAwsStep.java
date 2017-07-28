package com.latticeengines.workflow.core;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.serviceflows.datacloud.etl.steps.AWSBatchConfiguration;

@Component("dummyAwsStep")
public class DummyAwsStep extends BaseAwsBatchStep<AWSBatchConfiguration> {

    private static final Log log = LogFactory.getLog(DummyAwsStep.class);

    @Override
    public void executeInline() {
        AWSBatchConfiguration config = getConfiguration();
        log.info("Inside DummyAwsStep." + " Config=" + config);
    }
}
