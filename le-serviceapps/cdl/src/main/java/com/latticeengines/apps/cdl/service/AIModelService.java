package com.latticeengines.apps.cdl.service;

import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public interface AIModelService extends RatingModelService<AIModel> {

    EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine, AIModel aiModel,
            ModelingQueryType modelingQueryType, DataCollection.Version version);

    void updateModelingJobStatus(String ratingEngineId, String aiModelId, JobStatus newStatus);
}
