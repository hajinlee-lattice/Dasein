package com.latticeengines.scoringapi.scoringtest;

import java.util.List;
import java.util.Map;

import com.latticeengines.scoringapi.unused.CombinationElement;
import com.latticeengines.scoringapi.unused.ScoreType;

public class TestDefinition {
    public String testName;
    public List<Map<String,Object>> records;
    public List<Map<ScoreType,Object>> scores;
    public List<CombinationElement> combination;
    public Map<String,String> modelsPMML;
}
