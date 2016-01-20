package com.latticeengines.propdata.collection.dataflow.pivot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.domain.exposed.dataflow.BooleanType;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;

import cascading.operation.Buffer;
import cascading.tuple.Fields;

@Component("builtWithPivotFlow")
public class BuiltWithPivotFlow extends PivotFlow {

    private static final Long ONE_MONTH = TimeUnit.DAYS.toMillis(30);

    private static ObjectMapper mapper = new ObjectMapper();

    private String domainField;

    @Override
    public Node construct(PivotDataFlowParameters parameters) {
        domainField = parameters.getJoinFields()[0];

        Node source = addSource(parameters.getBaseTables().get(0));
        Map<String, Node> sourceMap = new HashMap<>();
        sourceMap.put(parameters.getBaseTables().get(0), source);

        Node oneMonth = source.filter("Technology_Last_Detected + " + ONE_MONTH + "L >= LE_Last_Upload_Date",
                new FieldList("Technology_Last_Detected", "LE_Last_Upload_Date"));
        oneMonth = oneMonth.renamePipe("one-month");
        sourceMap.put(parameters.getBaseTables().get(0) + "_LastMonth", oneMonth);

        Node join = joinedConfigurablePipes(parameters, sourceMap);
        Node topAttrs = pivotTopAttributes(source, parameters.getColumns());

        PivotStrategy pivotStrategy = new BuiltWithPivotStrategy();
        Node pivot = source.pivot(parameters.getJoinFields(), pivotStrategy);

        for (String typeField: BuiltWithPivotStrategy.typeFlags) {
            pivot = pivot.renameBooleanField(typeField, BooleanType.TRUE_FALSE);
        }

        Node recent = pivotRecentTechTag(source);

        FieldList joinList = new FieldList(parameters.getJoinFields());
        pivot = pivot.join(joinList, join, joinList, JoinType.OUTER);
        pivot = pivot.join(joinList, topAttrs, joinList, JoinType.OUTER);
        pivot = pivot.join(joinList, recent, joinList, JoinType.OUTER);

        pivot = pivot.addTimestamp(parameters.getTimestampField());
        return finalRetain(pivot, parameters.getColumns());
    }

    private Node pivotRecentTechTag(Node source) {
        Node threeMonth = source.filter("Technology_First_Detected + " + ONE_MONTH * 3 + "L >= LE_Last_Upload_Date",
                new FieldList("Technology_First_Detected", "LE_Last_Upload_Date"));
        threeMonth = threeMonth.renamePipe("recent-tech");

        Buffer<?> buffer = new BuiltWithRecentTechBuffer(new Fields("Domain",
                "BusinessTechnologiesRecentTechnologies", "BusinessTechnologiesRecentTags"));
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(domainField, String.class));
        fms.add(new FieldMetadata("BusinessTechnologiesRecentTechnologies", String.class));
        fms.add(new FieldMetadata("BusinessTechnologiesRecentTags", String.class));
        return threeMonth.groupByAndBuffer(new FieldList(domainField), buffer, fms);
    }

    private Node pivotTopAttributes(Node source, List<SourceColumn> columns) {
        Map<String, String> attrMap = topAttrFields(columns);
        List<FieldMetadata> fms = new ArrayList<>();
        fms.add(new FieldMetadata(domainField, String.class));
        fms.addAll(topAttrFieldMetadata(columns));

        List<String> fieldNames = new ArrayList<>();
        for (FieldMetadata fm: fms) { fieldNames.add(fm.getFieldName()); }

        Buffer<?> buffer = new BuiltWithTopAttrBuffer(attrMap,
                new Fields(fieldNames.toArray(new String[fieldNames.size()])));
        Node topAttrs = source.groupByAndBuffer(new FieldList(domainField), buffer, fms);
        topAttrs = topAttrs.renamePipe("top-attr");
        return topAttrs;
    }

    private Map<String, String> topAttrFields(List<SourceColumn> columns) {
        Map<String, String> attrs = new HashMap<>();
        for (SourceColumn column: columns) {
            if (column.getCalculation().equals(SourceColumn.Calculation.BUILTWITH_TOPATTR)) {
                try {
                    JsonNode argNode = mapper.readTree(column.getArguments());
                    String[] names = argNode.get("TechnologyNames").asText().split(",");
                    for (String name: names) {
                        attrs.put(name, column.getColumnName());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        return attrs;
    }

    private List<FieldMetadata> topAttrFieldMetadata(List<SourceColumn> columns) {
        List<FieldMetadata> fms = new ArrayList<>();
        for (SourceColumn column: columns) {
            if (column.getCalculation().equals(SourceColumn.Calculation.BUILTWITH_TOPATTR)) {
                fms.add(new FieldMetadata(column.getColumnName(), String.class));
            }
        }
        return fms;
    }

}
