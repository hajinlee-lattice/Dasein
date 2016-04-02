package com.latticeengines.skald.exposed;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;
import com.latticeengines.skald.exposed.domain.ModelTags;

public class ScoreRequest {
    public ScoreRequest(CustomerSpace space, String combination, Map<String, Object> record) {
        this.space = space;
        this.combination = combination;
        this.record = record;
        this.tag = ModelTags.ACTIVE;
    }

    public ScoreRequest() {
        this.tag = ModelTags.ACTIVE;
    }

    public CustomerSpace space;

    // Combination of models that is the target of this score request.
    public String combination;

    // Which tagged version of the models to score against.
    public String tag;

    public Map<String, Object> record;
}
