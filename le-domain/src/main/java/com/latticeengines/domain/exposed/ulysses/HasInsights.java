package com.latticeengines.domain.exposed.ulysses;

import java.util.List;

public interface HasInsights {
    List<Insight> getInsights();

    void setInsights(List<Insight> insights);

    List<InsightAttribute> getInsightModifiers();

    void setInsightModifiers(List<InsightAttribute> insights);

}
