package com.latticeengines.scoringinternalapi.controller;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.ScoreRequest;
import com.latticeengines.monitor.exposed.ratelimit.RateCounter;

@Component
public class ScoreRequestRateLimiter extends RateCounter {

    @Value("${scoringapi.score.ratelimit:4000}")
    private int ratelimit;

    @Override
    protected int getRatelimit() {
        return ratelimit;
    }

    @Override
    public int getRecordCount(Object[] args) {
        int recordCount = 0;
        for (Object arg : args) {
            if (arg instanceof ScoreRequest) {
                recordCount = 1;
                break;
            } else if (arg instanceof BulkRecordScoreRequest) {
                recordCount = ((BulkRecordScoreRequest) arg).getRecords().size();
                break;
            }
        }

        return recordCount;
    }
}
