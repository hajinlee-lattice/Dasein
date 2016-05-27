package com.latticeengines.network.exposed.scoringapi;

import java.util.List;

import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;

public interface ScoringApiInterface {

    List<RecordScoreResponse> bulkScore(BulkRecordScoreRequest scoreRequest);
}
