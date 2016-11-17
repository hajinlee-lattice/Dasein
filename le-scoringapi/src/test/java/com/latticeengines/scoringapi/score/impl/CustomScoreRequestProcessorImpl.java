package com.latticeengines.scoringapi.score.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.springframework.stereotype.Component;

import com.google.common.base.Joiner;
import com.latticeengines.domain.exposed.scoringapi.Warning;
import com.latticeengines.domain.exposed.scoringapi.WarningCode;

@Component("customScoreRequestProcessor")
public class CustomScoreRequestProcessorImpl extends ScoreRequestProcessorImpl {
    private HashMap<String, List<Warning>> warningMap = new HashMap<>();

    @Override
    protected List<Warning> getWarnings(String recordId) {
        return warningMap.get(recordId);
    }

    @Override
    protected void addWarning(WarningCode code, String recordId, List<String> fields, String modelId) {
        List<Warning> warningList = warningMap.get(recordId);
        if (warningList == null) {
            warningList = new ArrayList<>();
            warningMap.put(recordId, warningList);
        }
        warningList.add(new Warning(code, new String[] { getWarningPrefix(modelId) + Joiner.on(",").join(fields) }));
    }
}
