package com.latticeengines.propdata.madison.service.impl;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("madisonDataFlowAggregationBuilder")
public class MadisonDataFlowBuilder extends CascadingDataFlowBuilder {

    public MadisonDataFlowBuilder() {
        super(false, false);
    }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);

        addSource("MadisonLogicForToday", sources.get("MadisonLogic0"));
        String lastAggregatedOperatorName = aggregateTodayData();

        String pastDateData = sources.get("MadisonLogic1");
        if (pastDateData == null) {
            lastAggregatedOperatorName = createPctChangeCollumns(lastAggregatedOperatorName);
        } else {
            addSource("MadisonLogicForYesterday", pastDateData);
            lastAggregatedOperatorName = joinWithLastDateData();
        }

        return lastAggregatedOperatorName;
    }

    private String aggregateTodayData() {
        String lastAggregatedOperatorName;
        List<GroupByCriteria> groupByCriteria = new ArrayList<>();

        groupByCriteria.add(new GroupByCriteria("Topic", "Topic_Total", GroupByCriteria.AggregationType.COUNT));
        lastAggregatedOperatorName = addGroupBy("MadisonLogicForToday", new FieldList("DomainID", "Category",
                "HashedEmailID"), groupByCriteria);

        groupByCriteria = new ArrayList<>();
        groupByCriteria.add(new GroupByCriteria("Topic_Total", "ML_30Day_Category_Total",
                GroupByCriteria.AggregationType.SUM));
        groupByCriteria.add(new GroupByCriteria("HashedEmailID", "ML_30Day_Category_UniqueUsers",
                GroupByCriteria.AggregationType.COUNT));
        lastAggregatedOperatorName = addGroupBy(lastAggregatedOperatorName, new FieldList("DomainID", "Category"),
                groupByCriteria);

        FieldMetadata fieldMetaData = new FieldMetadata(Type.INT, Integer.class, "ML_30Day_Category_Total", null);
        lastAggregatedOperatorName = addFunction(
                lastAggregatedOperatorName, //
                "ML_30Day_Category_Total != null ? new Integer(ML_30Day_Category_Total.intValue()) : null ",
                new FieldList("ML_30Day_Category_Total"), //
                fieldMetaData);
        return lastAggregatedOperatorName;
    }

    private String createPctChangeCollumns(String lastAggregatedOperatorName) {
        FieldMetadata fieldMetaData = new FieldMetadata(Type.DOUBLE, Float.class, "ML_30Day_Category_Total_PctChange",
                null);
        lastAggregatedOperatorName = addFunction(lastAggregatedOperatorName, //
                "null", //
                new FieldList(new String[0]), //
                fieldMetaData);
        fieldMetaData = new FieldMetadata(Type.DOUBLE, Float.class, "ML_30Day_Category_UniqueUsers_PctChange", null);
        lastAggregatedOperatorName = addFunction(lastAggregatedOperatorName, //
                "null", //
                new FieldList(new String[0]), //
                fieldMetaData);
        return lastAggregatedOperatorName;
    }

    private String joinWithLastDateData() {

        String lastAggregatedOperatorName = addRetainFunction("MadisonLogicForYesterday", new FieldList(new String[] {
                "DomainID", "Category", "ML_30Day_Category_Total", "ML_30Day_Category_UniqueUsers" }));

        lastAggregatedOperatorName = addLeftOuterJoin("MadisonLogicForToday", new FieldList("DomainID", "Category"),
                lastAggregatedOperatorName, new FieldList("DomainID", "Category"));
        FieldMetadata fieldMetaData = new FieldMetadata(Type.DOUBLE, Float.class, "ML_30Day_Category_Total_PctChange",
                null);
        lastAggregatedOperatorName = addFunction(
                lastAggregatedOperatorName, //
                "MadisonLogicForYesterday__ML_30Day_Category_Total != null ? "
                        + "new Double((ML_30Day_Category_Total.doubleValue() - MadisonLogicForYesterday__ML_30Day_Category_Total.doubleValue())/MadisonLogicForYesterday__ML_30Day_Category_Total.doubleValue()) : null", //
                new FieldList("ML_30Day_Category_Total", "MadisonLogicForYesterday__ML_30Day_Category_Total"), //
                fieldMetaData);

        fieldMetaData = new FieldMetadata(Type.DOUBLE, Float.class, "ML_30Day_Category_UniqueUsers_PctChange", null);
        lastAggregatedOperatorName = addFunction(
                lastAggregatedOperatorName, //
                "MadisonLogicForYesterday__ML_30Day_Category_UniqueUsers != null ? "
                        + "new Double((ML_30Day_Category_UniqueUsers.doubleValue() - "
                        + "MadisonLogicForYesterday__ML_30Day_Category_UniqueUsers.doubleValue())/MadisonLogicForYesterday__ML_30Day_Category_UniqueUsers.doubleValue()) : null", //
                new FieldList("ML_30Day_Category_UniqueUsers",
                        "MadisonLogicForYesterday__ML_30Day_Category_UniqueUsers"), fieldMetaData, new FieldList(
                        new String[] { "DomainID", "Category", "ML_30Day_Category_Total",
                                "ML_30Day_Category_UniqueUsers", "ML_30Day_Category_Total_PctChange",
                                "ML_30Day_Category_UniqueUsers_PctChange" }));
        return lastAggregatedOperatorName;
    }
}
