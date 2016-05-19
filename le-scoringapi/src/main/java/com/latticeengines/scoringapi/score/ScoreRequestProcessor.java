package com.latticeengines.scoringapi.score;

import java.util.List;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.scoringapi.exposed.BulkRecordScoreRequest;
import com.latticeengines.scoringapi.exposed.RecordScoreResponse;
import com.latticeengines.scoringapi.exposed.ScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;

public interface ScoreRequestProcessor {

    ScoreResponse process(CustomerSpace space, ScoreRequest request, boolean isDebug);

    List<RecordScoreResponse> process(CustomerSpace customerSpace, BulkRecordScoreRequest scoreRequests,
            boolean isDebug);
}
