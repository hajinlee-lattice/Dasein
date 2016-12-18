package com.latticeengines.scoringapi.score;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

public interface ScoreRequestProcessor {

    ScoreResponse process(CustomerSpace space, ScoreRequest request, boolean isDebug, //
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId,
            boolean isCalledViaApiConsole);

    ScoreResponse process(CustomerSpace space, ScoreRequest request, boolean isDebug, //
            boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId,
            boolean isCalledViaApiConsole, boolean enforceFuzzyMatch);

    List<RecordScoreResponse> process(CustomerSpace customerSpace, BulkRecordScoreRequest scoreRequests,
            boolean isDebug, boolean enrichInternalAttributes, boolean performFetchOnlyForMatching, String requestId);
}
