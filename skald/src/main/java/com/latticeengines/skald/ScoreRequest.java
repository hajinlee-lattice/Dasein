package com.latticeengines.skald;

import java.util.Map;

import com.latticeengines.domain.exposed.camille.CustomerSpace;

public class ScoreRequest {
    public CustomerSpace space;

    // Combination of models that is the target of this score request.
    public String combination;

    public Map<String, Object> record;
}
