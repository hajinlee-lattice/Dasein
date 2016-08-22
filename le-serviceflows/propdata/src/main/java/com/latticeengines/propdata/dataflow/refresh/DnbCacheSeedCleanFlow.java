package com.latticeengines.propdata.dataflow.refresh;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMapping;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMappingImpl;
import com.latticeengines.dataflow.runtime.cascading.propdata.DomainMergeAndCleanFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.TypeConvertFunction;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.propdata.dataflow.CleanDataFlowParameter;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;

@Component("dnbCacheSeedCleanFlow")
public class DnbCacheSeedCleanFlow extends TypesafeDataFlowBuilder<CleanDataFlowParameter> {
    @Override
    public Node construct(CleanDataFlowParameter parameters) {
        Node node = addSource(parameters.getBaseTables().get(0));
        node = addDataCleanNode(node, parameters.getColumns());
        node = addColumnNode(node, parameters.getColumns());
        return node;
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

    private void setSchemaToNode(Node node, List<SourceColumn> baseSourceColumns) {
        CsvToAvroFieldMapping fieldMapping = new CsvToAvroFieldMappingImpl(baseSourceColumns);
        Schema schema = fieldMapping.getAvroSchema();

        List<FieldMetadata> fieldMetadataList = new ArrayList<>();
        for (Field field : schema.getFields()) {
            for (Schema fieldType : field.schema().getTypes()) {
                if (fieldType.getType() == Type.NULL) {
                    continue;
                }
                Class<?> javaType = AvroUtils.getJavaType(fieldType.getType());
                fieldMetadataList.add(new FieldMetadata(field.name(), javaType));
                break;
            }
        }
        node.setSchema(fieldMetadataList);
    }
}
