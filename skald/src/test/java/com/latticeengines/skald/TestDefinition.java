package com.latticeengines.skald;

import java.util.List;
import java.util.Map;

public class TestDefinition {
    public String testName;
    public List<Map<String,Object>> records;
    public List<Map<ScoreType,Object>> scores;
    public List<CombinationElement> combination;
    public Map<String,String> modelsPMML;
}
