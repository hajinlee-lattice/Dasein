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
        distinctValueCount.setDisplayName("Too many category values");
        distinctValueCount.setDescription(
                "This attribute has more than 200 category values. Attributes with more than 200 different category values cannot be used in modeling. Where possible, replace with a picklist attribute instead of a free field.");
        distinctValueCount.setMandatoryRemoval(false);
        ruleList.add(distinctValueCount);

        DataRule valuePercentage = new DataRule("ValuePercentage");
        valuePercentage.setDisplayName("Too many identical values");
        valuePercentage.setDescription(
                "This attribute has the same value for 98% or more records. This can lead to poor segments or inaccurate scores.");
        valuePercentage.setMandatoryRemoval(false);
        ruleList.add(valuePercentage);

        DataRule nullLift = new DataRule("NullLift");
        nullLift.setDisplayName("Prediction from missing data");
        nullLift.setDescription(
                "This attribute brings prediction from missing data into the model. When unpopulated numbers or categories show signifiant prediction (less than 0.7 or greater than 1.2), later scores are often inaccurate. ");
        nullLift.setMandatoryRemoval(false);
        ruleList.add(nullLift);

        DataRule futureInfo = new DataRule("FutureInfo");
        futureInfo.setDisplayName("Prediction from later data (future information)");
        futureInfo.setDescription(
                "This attribute looks like it was populated later in the business cycle, often called future information. This warning comes when available values show good lift (greater than 1.5), but 80% or more records are unpopulated and have low lift (below 0.6).");
        futureInfo.setMandatoryRemoval(false);
        ruleList.add(futureInfo);

        return ruleList;
    }
}