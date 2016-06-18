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
    List<Warning> getWarnings(String recordId) {
        return warningMap.get(recordId);
    }

    @Override
    void addWarning(String recordId, List<String> missingFields) {
        List<Warning> warningList = warningMap.get(recordId);
        if (warningList == null) {
            warningList = new ArrayList<>();
            warningMap.put(recordId, warningList);
        }
        warningList.add(new Warning(WarningCode.MISSING_COLUMN, new String[] { Joiner.on(",").join(missingFields) }));
    }
}
