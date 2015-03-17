package com.latticeengines.madison.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("madisonDataFlowBuilder")
public class MadisonDataFlowBuilder extends CascadingDataFlowBuilder {

    public MadisonDataFlowBuilder() {
        super(false, false);
    }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);

        addSource("MadisonLogicForToday", sources.get("MadisonLogic0"));

        String lastAggregatedOperatorName = null;
        List<GroupByCriteria> groupByCriteria = new ArrayList<>();

        groupByCriteria.add(new GroupByCriteria("Topic", "Topic_Total", GroupByCriteria.AggregationType.COUNT));
        lastAggregatedOperatorName = addGroupBy("MadisonLogicForToday", new FieldList("DomainID", "Category",
                "HashedEmailID"), groupByCriteria);

        groupByCriteria = new ArrayList<>();
        groupByCriteria.add(new GroupByCriteria("HashedEmailID", "ML_30Day_Category_UniqueUsers",
                GroupByCriteria.AggregationType.COUNT));
        groupByCriteria.add(new GroupByCriteria("Topic_Total", "ML_30Day_Category_TOTAL",
                GroupByCriteria.AggregationType.SUM));
        lastAggregatedOperatorName = addGroupBy(lastAggregatedOperatorName, new FieldList("DomainID", "Category"),
                groupByCriteria);

        String yesterdayData = sources.get("MadisonLogic1");
        if (yesterdayData != null) {
            addSource("MadisonLogicForYesterday", yesterdayData);
            lastAggregatedOperatorName = addLeftOuterJoin("MadisonLogicForToday",
                    new FieldList("DomainID", "Category"), "MadisonLogicForYesterday", new FieldList("DomainID",
                            "Category"));
        }

        return lastAggregatedOperatorName;
    }
}
