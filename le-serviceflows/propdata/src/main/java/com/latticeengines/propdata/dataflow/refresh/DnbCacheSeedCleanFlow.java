package com.latticeengines.propdata.dataflow.refresh;

import java.util.Arrays;
import java.util.List;

import org.springframework.stereotype.Component;

import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainMergeAndCleanFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.TypeConvertFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.dataflow.SingleBaseSourceRefreshDataFlowParameter;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;

@Component("dnbCacheSeedCleanFlow")
public class DnbCacheSeedCleanFlow extends TypesafeDataFlowBuilder<SingleBaseSourceRefreshDataFlowParameter> {

    private final static String DUNS_FIELD = "DUNS_NUMBER";

    @Override
    public Node construct(SingleBaseSourceRefreshDataFlowParameter parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));
        node = addFilterNode(node);
        node = addDataCleanNode(node, parameters.getColumns());
        node = addDedupNode(node, parameters.getJoinFields());
        node = addColumnNode(node, parameters.getColumns());
        return node;
    }

    private Node addFilterNode(Node node) {
        return node.filter(DUNS_FIELD + " != null", new FieldList(DUNS_FIELD));
    }

    private Node addDedupNode(Node node, String[] dedupColumns) {
        return node.groupByAndLimit(new FieldList(dedupColumns), 1);
    }

    private Node addDataCleanNode(Node node, List<SourceColumn> sourceColumns) {
        for (SourceColumn sourceColumn : sourceColumns) {
            switch (sourceColumn.getCalculation()) {
                case STANDARD_DOMAIN:
                    List<String> domainFieldNames = Arrays.asList(sourceColumn.getColumnName());
                    node = node.apply(new DomainMergeAndCleanFunction(domainFieldNames, sourceColumn.getColumnName()), new FieldList(domainFieldNames), new FieldMetadata(sourceColumn.getColumnName(), String.class));
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
