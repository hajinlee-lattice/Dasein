package com.latticeengines.propdata.collection.service;

import org.apache.hadoop.yarn.api.records.ApplicationId;

import com.latticeengines.domain.exposed.propdata.manage.PublicationProgress;

public interface PublicationExecutor {

    ApplicationId executeWorkflow(PublicationProgress progress);

}
