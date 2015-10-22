package com.latticeengines.workflow.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.stereotype.Component;

import com.latticeengines.workflow.build.AbstractStep;

@Component("modelProfileData")
public class ModelProfileData extends AbstractStep {

    private static final Log log = LogFactory.getLog(ModelProfileData.class);

    @Override
    public void execute() {
        log.info("Inside ModelProfileData execute()");
    }

}
