package com.latticeengines.propdata.workflow.collection.steps;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import com.latticeengines.propdata.core.service.impl.HdfsPathBuilder;
import com.latticeengines.proxy.exposed.propdata.InternalProxy;
import com.latticeengines.serviceflows.workflow.core.BaseWorkflowStep;

@Component("publish")
public class Publish extends BaseWorkflowStep<PublishConfiguration> {

    public static final Log log = LogFactory.getLog(Publish.class);

    @Autowired
    private HdfsPathBuilder hdfsPathBuilder;

    @Autowired
    private InternalProxy internalProxy;

    @Override
    public void execute() {
        log.info("Inside Publish execute()");
        log.info(getConfiguration().getPublicationConfiguration());
        hdfsPathBuilder.changeHdfsPodId(getConfiguration().getHdfsPodId());
    }

}
