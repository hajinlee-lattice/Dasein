package com.latticeengines.proxy.exposed.scoringapi;

import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.domain.exposed.scoringapi.BulkRecordScoreRequest;
import com.latticeengines.domain.exposed.scoringapi.RecordScoreResponse;
import com.latticeengines.network.exposed.scoringapi.ScoringApiInterface;
import com.latticeengines.proxy.exposed.BaseRestApiProxy;

@Component("scoringApiProxy")
public class ScoringApiProxy extends BaseRestApiProxy implements ScoringApiInterface {

    public ScoringApiProxy() {
        super("score");
    }

    @SuppressWarnings("unchecked")
    @Override
    public List<RecordScoreResponse> bulkScore(BulkRecordScoreRequest scoreRequest) {
        String url = constructUrl("/records");
        return post("scoreRTSBulk", url, scoreRequest, List.class);
    }
}
