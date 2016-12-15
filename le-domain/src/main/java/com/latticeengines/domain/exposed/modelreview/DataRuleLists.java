package com.latticeengines.domain.exposed.modelreview;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class DataRuleLists {

    public static List<DataRule> getDataRules(DataRuleListName listName) {
        switch (listName) {
        case NONE:
            return Collections.emptyList();
        case STANDARD:
        default:
            return getStandardDataRules();
        }
    }

    public static List<DataRule> getStandardDataRules() {
        List<DataRule> ruleList = new ArrayList<>();

        DataRule distinctValueCount = new DataRule("DistinctValueCount");
        distinctValueCount.setDisplayName("Distinct Value Count");
        distinctValueCount.setDescription("Categorical attributes may not have more than 200 distinct values");
        distinctValueCount.setMandatoryRemoval(true);
        ruleList.add(distinctValueCount);

        return ruleList;
    }
}