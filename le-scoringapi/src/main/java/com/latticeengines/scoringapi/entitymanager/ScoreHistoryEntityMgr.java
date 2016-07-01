package com.latticeengines.scoringapi.entitymanager;

import com.latticeengines.datafabric.entitymanager.BaseFabricEntityMgr;
import com.latticeengines.domain.exposed.scoringapi.Record;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.domain.exposed.scoringapi.ScoreRecordHistory;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreResponse;

import java.util.List;

public interface ScoreHistoryEntityMgr extends BaseFabricEntityMgr<ScoreRecordHistory> {

    void publish(List<Record> requests, List<RecordScoreResponse> responses);
    void publish(Record request, RecordScoreResponse response);
    void publish(ScoreRequest request, ScoreResponse response);

    List<ScoreRecordHistory> findByLatticeId(String latticeId);
}
