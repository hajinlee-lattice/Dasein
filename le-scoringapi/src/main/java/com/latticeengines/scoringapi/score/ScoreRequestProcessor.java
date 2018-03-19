package com.latticeengines.scoringapi.score;

import java.util.List;

import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

public interface ScoreRequestProcessor {

    ScoreResponse process(ScoreRequest request, AdditionalScoreConfig additionalScoreConfig);

    List<RecordScoreResponse> process(BulkRecordScoreRequest scoreRequests,
            AdditionalScoreConfig additionalScoreConfig);
}
