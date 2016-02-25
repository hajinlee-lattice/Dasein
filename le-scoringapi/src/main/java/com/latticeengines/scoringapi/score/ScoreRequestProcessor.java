package com.latticeengines.scoringapi.score;

import com.latticeengines.scoringapi.exposed.ContactScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;

public interface ScoreRequestProcessor {

    ScoreResponse process(ContactScoreRequest request);
}
