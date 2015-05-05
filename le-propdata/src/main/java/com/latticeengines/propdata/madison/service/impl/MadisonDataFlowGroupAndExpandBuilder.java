package com.latticeengines.propdata.madison.service.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.CascadingDataFlowBuilder;
import com.latticeengines.dataflow.exposed.exception.DataFlowCode;
import com.latticeengines.dataflow.exposed.exception.DataFlowException;
import com.latticeengines.domain.exposed.dataflow.DataFlowContext;

@Component("madisonDataFlowGroupAndExpandBuilder")
public class MadisonDataFlowGroupAndExpandBuilder extends CascadingDataFlowBuilder {

    public MadisonDataFlowGroupAndExpandBuilder() {
        super(false, false);
    }

    @Override
    public String constructFlowDefinition(DataFlowContext dataFlowCtx, Map<String, String> sources) {
        setDataFlowCtx(dataFlowCtx);

        addSource("MadisonLogicForToday", sources.get("MadisonLogic0"));

        String lastAggregatedOperatorName = addGroupByAndExpand("MadisonLogicForToday", new FieldList("DomainID"),
                "Category", Arrays.asList(new String[] { "ML_30Day_%s_Total", "ML_30Day_%s_UniqueUsers",
                        "ML_30Day_%s_Total_PctChange", "ML_30Day_%s_UniqueUsers_PctChange" }), new FieldList(
                        new String[] { "Category", "ML_30Day_Category_Total", "ML_30Day_Category_UniqueUsers",
                                "ML_30Day_Category_Total_PctChange", "ML_30Day_Category_UniqueUsers_PctChange" }),
                buildDeclaredFieldList(dataFlowCtx));

        return lastAggregatedOperatorName;
    }

    private FieldList buildDeclaredFieldList(DataFlowContext dataFlowCtx) {
        Schema schema = getSchemaFromFile(dataFlowCtx);
        if (schema == null) {
            throw new DataFlowException(DataFlowCode.DF_10004);
        }
        List<String> fieldNames = new ArrayList<>();
        for (Field field : schema.getFields()) {
            fieldNames.add(field.name());
        }
        return new FieldList(fieldNames.toArray(new String[0]));
    }

}
