package com.latticeengines.datacloud.dataflow.transformation.pivot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import org.springframework.stereotype.Component;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.datacloud.dataflow.transformation.ConfigurableFlowBase;
import com.latticeengines.datacloud.dataflow.transformation.pivot.utils.PivotUtils;
import com.latticeengines.datacloud.dataflow.utils.FlowUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.exposed.builder.common.JoinType;
import com.latticeengines.dataflow.exposed.builder.strategy.PivotStrategy;
import com.latticeengines.dataflow.runtime.cascading.propdata.BuiltWithPivotStrategy;
import com.latticeengines.dataflow.runtime.cascading.propdata.BuiltWithRecentTechBuffer;
import com.latticeengines.dataflow.runtime.cascading.propdata.BuiltWithTopAttrBuffer;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.PivotTransformerConfig;
import com.latticeengines.domain.exposed.datacloud.transformation.config.impl.TransformerConfig;
import com.latticeengines.domain.exposed.dataflow.BooleanType;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

import cascading.operation.Buffer;
import cascading.tuple.Fields;

@Component(PivotBuiltWith.BEAN_NAME)
public class PivotBuiltWith extends ConfigurableFlowBase<PivotTransformerConfig> {

    public static final String BEAN_NAME = "pivotBuiltWith";

    private String rowIdField = "RowId" + UUID.randomUUID().toString().replace("-", "");

    private static final Long ONE_MONTH = TimeUnit.DAYS.toMillis(30);

    private static ObjectMapper mapper = new ObjectMapper();

    private String domainField;

    @Override
    public Class<? extends TransformerConfig> getTransformerConfigClass() {
        return PivotTransformerConfig.class;
    }

    @Override
    public String getDataFlowBeanName() {
        return BEAN_NAME;
    }

    @Override
    public String getTransformerName() {
        return "pivotTransformer";
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        PivotTransformerConfig config = getTransformerConfig(parameters);
        domainField = config.getJoinFields()[0];

        Node source = addSource(parameters.getBaseTables().get(0));
        Map<String, Node> sourceMap = new HashMap<>();
        sourceMap.put(parameters.getBaseTables().get(0), source);

        Node oneMonth = source.filter(
                "Technology_Last_Detected + " + ONE_MONTH + "L >= CollectedAt",
                new FieldList("Technology_Last_Detected", "CollectedAt"));
        oneMonth = oneMonth.renamePipe("one-month");
        sourceMap.put(parameters.getBaseTables().get(0) + "_LastMonth", oneMonth);

        Node join = PivotUtils.joinedConfigurablePipes(parameters.getColumns(), config.getJoinFields(), sourceMap, rowIdField);
        Node topAttrs = pivotTopAttributes(source, parameters.getColumns());

        PivotStrategy pivotStrategy = new BuiltWithPivotStrategy();
        Node pivot = source.pivot(config.getJoinFields(), pivotStrategy);

        for (String typeField : BuiltWithPivotStrategy.typeFlags) {
            pivot = pivot.renameBooleanField(typeField, BooleanType.TRUE_FALSE);
        }

        Node recent = pivotRecentTechTag(source);

        FieldList joinList = new FieldList(config.getJoinFields());
        pivot = pivot.join(joinList, join, joinList, JoinType.OUTER);
        pivot = pivot.join(joinList, topAttrs, joinList, JoinType.OUTER);
        pivot = pivot.join(joinList, recent, joinList, JoinType.OUTER);
        pivot = FlowUtils.removeInvalidDatetime(pivot, parameters.getColumns());
        pivot = FlowUtils.truncateStringFields(pivot, parameters.getColumns());
        pivot = pivot.addTimestamp(parameters.getTimestampField());
        return PivotUtils.finalRetain(pivot, parameters.getColumns());
    }

    private Node pivotRecentTechTag(Node source) {
        Node threeMonth = source.filter(
                "Technology_First_Detected + " + ONE_MONTH * 3 + "L >= CollectedAt",
                new FieldList("Technology_First_Detected", "CollectedAt"));
        threeMonth = threeMonth.renamePipe("recent-tech");

        Buffer<?> buffer = new BuiltWithRecentTechBuffer(
                new Fields("Domain", "BusinessTechnologiesRecentTechnologies",
                "BusinessTechnologiesRecentTags"));
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
        for (FieldMetadata fm : fms) {
            fieldNames.add(fm.getFieldName());
        }

        Buffer<?> buffer = new BuiltWithTopAttrBuffer(attrMap, new Fields(fieldNames.toArray(new String[fieldNames
                .size()])));
        Node topAttrs = source.groupByAndBuffer(new FieldList(domainField), buffer, fms);
        topAttrs = topAttrs.renamePipe("top-attr");
        return topAttrs;
    }

    private Map<String, String> topAttrFields(List<SourceColumn> columns) {
        Map<String, String> attrs = new HashMap<>();
        for (SourceColumn column : columns) {
            if (column.getCalculation().equals(SourceColumn.Calculation.BUILTWITH_TOPATTR)) {
                try {
                    JsonNode argNode = mapper.readTree(column.getArguments());
                    String[] names = argNode.get("TechnologyNames").asText().split(",");
                    for (String name : names) {
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
        for (SourceColumn column : columns) {
            if (column.getCalculation().equals(SourceColumn.Calculation.BUILTWITH_TOPATTR)) {
                fms.add(new FieldMetadata(column.getColumnName(), String.class));
            }
        }
        return fms;
    }

}
