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

        DataRule valuePercentage = new DataRule("ValuePercentage");
        valuePercentage.setDisplayName("Value Percentage");
        valuePercentage
                .setDescription("Attributes may not have a single value with a population rate above a threshold");
        valuePercentage.setMandatoryRemoval(false);
        ruleList.add(valuePercentage);

        DataRule nullLift = new DataRule("NullLift");
        nullLift.setDisplayName("Lift from NULL");
        nullLift.setDescription("Attributes may not indicate significant lift based on NULL values");
        nullLift.setMandatoryRemoval(false);
        ruleList.add(nullLift);

        DataRule futureInfo = new DataRule("FutureInfo");
        futureInfo.setDisplayName("Future Information");
        futureInfo.setDescription("Attributes may not be populated after a buying event");
        futureInfo.setMandatoryRemoval(false);
        ruleList.add(futureInfo);

        return ruleList;
    }
}