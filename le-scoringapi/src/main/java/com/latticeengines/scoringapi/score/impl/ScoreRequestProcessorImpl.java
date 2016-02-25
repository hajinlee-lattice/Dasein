package com.latticeengines.scoringapi.score.impl;

import org.springframework.stereotype.Component;

import com.latticeengines.scoringapi.exposed.ContactScoreRequest;
import com.latticeengines.scoringapi.exposed.ScoreResponse;
import com.latticeengines.scoringapi.score.ScoreRequestProcessor;

@Component("scoreRequestProcessor")
public class ScoreRequestProcessorImpl implements ScoreRequestProcessor {

    @Override
    public ScoreResponse process(ContactScoreRequest request) {
        // TODO Auto-generated method stub
        return null;
    }

}
