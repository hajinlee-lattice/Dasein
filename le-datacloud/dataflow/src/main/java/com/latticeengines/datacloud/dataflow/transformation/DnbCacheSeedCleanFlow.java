package com.latticeengines.datacloud.dataflow.transformation;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainMergeAndCleanFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.TypeConvertFunction;
import com.latticeengines.domain.exposed.datacloud.dataflow.TransformationFlowParameters;
import com.latticeengines.domain.exposed.datacloud.manage.SourceColumn;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.TransformationConfiguration;
import com.latticeengines.domain.exposed.datacloud.transformation.configuration.impl.BasicTransformationConfiguration;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;

@Component("dnbCacheSeedCleanFlow")
public class DnbCacheSeedCleanFlow
        extends TransformationFlowBase<BasicTransformationConfiguration, TransformationFlowParameters> {

    private final static String DUNS_FIELD = "DUNS_NUMBER";
    private final static String DOMESTIC_ULTIMATE_DUNS_NUMBER = "DOMESTIC_ULTIMATE_DUNS_NUMBER";
    private final static String LE_INDUSTRY = "LE_INDUSTRY";

    @Override
    protected Class<? extends TransformationConfiguration> getTransConfClass() {
        return BasicTransformationConfiguration.class;
    }

    @Override
    public Node construct(TransformationFlowParameters parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));
        node = addFilterNode(node);
        node = addDataCleanNode(node, parameters.getColumns());
        node = addDedupNode(node, parameters.getPrimaryKeys());
        node = addColumnNode(node, parameters.getColumns());
        return node;
    }

    private Node addFilterNode(Node node) {
        node = node.filter(DUNS_FIELD + " != null", new FieldList(DUNS_FIELD));
        return node.filter(
                DOMESTIC_ULTIMATE_DUNS_NUMBER + " != null || !(" + LE_INDUSTRY
                        + ".equals(\"Nonclassifiable Establishments\"))",
                new FieldList(DOMESTIC_ULTIMATE_DUNS_NUMBER, LE_INDUSTRY));
    }

    private Node addDedupNode(Node node, List<String> dedupColumns) {
        return node.groupByAndLimit(new FieldList(dedupColumns), 1);
    }

    private Node addDataCleanNode(Node node, List<SourceColumn> sourceColumns) {
        for (SourceColumn sourceColumn : sourceColumns) {
            switch (sourceColumn.getCalculation()) {
            case STANDARD_DOMAIN:
                List<String> domainFieldNames = Arrays.asList(sourceColumn.getColumnName());
                node = node.apply(new DomainMergeAndCleanFunction(domainFieldNames, sourceColumn.getColumnName()),
                        new FieldList(domainFieldNames), new FieldMetadata(sourceColumn.getColumnName(), String.class));
                break;
            case CONVERT_TYPE:
                String strategy = sourceColumn.getArguments();
                if (strategy.equals(TypeConvertFunction.ConvertTrategy.STRING_TO_INT.name())) {
                    TypeConvertFunction function = new TypeConvertFunction(sourceColumn.getColumnName(),
                            TypeConvertFunction.ConvertTrategy.STRING_TO_INT);
                    node = node.apply(function, new FieldList(sourceColumn.getColumnName()),
                            new FieldMetadata(sourceColumn.getColumnName(), Integer.class));
                } else if (strategy.equals(TypeConvertFunction.ConvertTrategy.STRING_TO_LONG.name())) {
                    TypeConvertFunction function = new TypeConvertFunction(sourceColumn.getColumnName(),
                            TypeConvertFunction.ConvertTrategy.STRING_TO_LONG);
                    node = node.apply(function, new FieldList(sourceColumn.getColumnName()),
                            new FieldMetadata(sourceColumn.getColumnName(), Long.class));
                } else {
                    throw new UnsupportedOperationException("Unknown type convert strategy: " + strategy);
                }
                break;
            default:
                break;
            }
        }
        return node;
    }

    private Node addColumnNode(Node node, List<SourceColumn> sourceColumns) {
        for (SourceColumn sourceColumn : sourceColumns) {
            switch (sourceColumn.getCalculation()) {
            case ADD_TIMESTAMP:
                node = node.addTimestamp(sourceColumn.getColumnName());
                break;
            default:
                break;
            }
        }
        return node;
    }
}
