package com.latticeengines.propdata.collection.service.impl;

import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;
import com.latticeengines.propdata.collection.service.PublicationExecutor;

@Component("publicationExecutor")
public class PublicationExecutorImpl implements PublicationExecutor {

    @Override
    public ApplicationId executeWorkflow(PublicationProgress progress) {
        return null;
    }

}
