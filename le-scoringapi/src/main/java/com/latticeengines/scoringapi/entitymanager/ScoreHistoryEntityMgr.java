package com.latticeengines.scoringapi.entitymanager;

import java.util.List;

import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

public interface ScoreHistoryEntityMgr {

    void publish(String tenantId, List<Record> requests, List<RecordScoreResponse> responses);

    void publish(String tenantId, Record request, RecordScoreResponse response);

    void publish(String tenantId, ScoreRequest request, ScoreResponse response);

}
