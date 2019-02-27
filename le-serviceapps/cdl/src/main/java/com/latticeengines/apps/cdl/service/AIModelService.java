package com.latticeengines.apps.cdl.service;

import java.util.List;
import java.util.Map;

import com.latticeengines.domain.exposed.cdl.ModelingQueryType;
import com.latticeengines.domain.exposed.datacloud.statistics.StatsCube;
import com.latticeengines.domain.exposed.metadata.ColumnMetadata;
import com.latticeengines.domain.exposed.metadata.DataCollection;
import com.latticeengines.domain.exposed.metadata.statistics.TopNTree;
import com.latticeengines.domain.exposed.pls.AIModel;
import com.latticeengines.domain.exposed.pls.RatingEngine;
import com.latticeengines.domain.exposed.pls.cdl.rating.model.CustomEventModelingConfig;
import com.latticeengines.domain.exposed.query.frontend.EventFrontEndQuery;
import com.latticeengines.domain.exposed.workflow.JobStatus;

public interface AIModelService extends RatingModelService<AIModel> {

    EventFrontEndQuery getModelingQuery(String customerSpace, RatingEngine ratingEngine,
            AIModel aiModel, ModelingQueryType modelingQueryType, DataCollection.Version version);

    void updateModelingJobStatus(String ratingEngineId, String aiModelId, JobStatus newStatus);

    Map<String, List<ColumnMetadata>> getIterationAttributes(String customerSpace,
            RatingEngine ratingEngine, AIModel aiModel,
            List<CustomEventModelingConfig.DataStore> dataStores);

    AIModel createNewIteration(AIModel aiModel, RatingEngine ratingEngine);

    List<ColumnMetadata> getIterationMetadata(String customerSpace, RatingEngine ratingEngine,
            AIModel aiModel, List<CustomEventModelingConfig.DataStore> dataStores);

    Map<String, StatsCube> getIterationMetadataCube(String customerSpace, RatingEngine ratingEngine,
            AIModel aiModel, List<CustomEventModelingConfig.DataStore> dataStores);

    TopNTree getIterationMetadataTopN(String customerSpace, RatingEngine ratingEngine,
            AIModel aiModel, List<CustomEventModelingConfig.DataStore> dataStores);

}
