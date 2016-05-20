package com.latticeengines.scoringapi.score;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreRequest;

public interface ScoreRequestProcessor {

    ScoreResponse process(CustomerSpace space, ScoreRequest request, boolean isDebug);

    List<RecordScoreResponse> process(CustomerSpace customerSpace, BulkRecordScoreRequest scoreRequests,
            boolean isDebug);
}
