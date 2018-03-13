package com.latticeengines.network.exposed.scoringapi;

import java.util.Date;
import java.util.List;

import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.DebugScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.Fields;
import com.latticeengines.domain.exposed.scoringapi.Model;
import com.latticeengines.domain.exposed.scoringapi.ModelDetail;
import com.latticeengines.domain.exposed.scoringapi.ModelType;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

public interface InternalScoringApiInterface {
    List<Model> getActiveModels(ModelType type, String tenantIdentifier);

    Fields getModelFields(String modelId, String tenantIdentifier);

    int getModelCount(Date lastUpdateTime, boolean considerAllStatus, String tenantIdentifier);

    ScoreResponse scorePercentileRecord(ScoreRequest scoreRequest, String tenantIdentifier,
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching);

    List<RecordScoreResponse> scorePercentileRecords(BulkRecordScoreRequest scoreRequest, String tenantIdentifier,
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, boolean enableMatching);

    DebugScoreResponse scoreProbabilityRecord(ScoreRequest scoreRequest, String tenantIdentifier,
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching);

    List<ModelDetail> getPaginatedModels(Date start, boolean considerAllStatus, int offset, int maximum,
            String tenantIdentifier);

    List<RecordScoreResponse> scorePercentileAndProbabilityRecords(BulkRecordScoreRequest scoreRequest,
            String tenantIdentifier, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching,
            boolean enableMatching);

    DebugScoreResponse scoreAndEnrichRecordApiConsole(ScoreRequest scoreRequest, String tenantIdentifier,
            boolean enrichInternalAttributes, boolean enforceFuzzyMatch, boolean skipDnBCache);
}
