package com.latticeengines.propdata.dataflow.depivot;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;
import org.apache.avro.Schema.Type;
import org.springframework.stereotype.Component;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.latticeengines.common.exposed.util.AvroUtils;
import com.latticeengines.dataflow.exposed.builder.Node;
import com.latticeengines.dataflow.exposed.builder.TypesafeDataFlowBuilder;
import com.latticeengines.dataflow.exposed.builder.common.FieldList;
import com.latticeengines.dataflow.runtime.cascading.StringTruncateFunction;
import com.latticeengines.dataflow.runtime.cascading.propdata.ColumnTypeConverter;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMapping;
import com.latticeengines.dataflow.runtime.cascading.propdata.CsvToAvroFieldMappingImpl;
import com.latticeengines.domain.exposed.dataflow.FieldMetadata;
import com.latticeengines.domain.exposed.exception.LedpCode;
import com.latticeengines.domain.exposed.exception.LedpException;
import com.latticeengines.domain.exposed.propdata.dataflow.DepivotDataFlowParameters;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn.Calculation;
import com.latticeengines.propdata.core.IngenstionNames;

import cascading.operation.Function;

@Component("bomboraDepivotFlow")
public class BomboraDepivotFlow extends TypesafeDataFlowBuilder<DepivotDataFlowParameters> {
    @Override
    public Node construct(DepivotDataFlowParameters parameters) {
        Node node = addSource(IngenstionNames.BOMBORA_FIREHOSE);
        setSchemaToNode(node, parameters.getBaseSourceColumns().get(0));
        try {
            node = addDepivotNode(node, parameters.getColumns());
        } catch (IOException e) {
            throw new LedpException(LedpCode.LEDP_25010, e);
        }
        node = node.addTimestamp(parameters.getTimestampField(), parameters.getTimestamp());
        node = addTruncateLogic(node, parameters.getColumns());
        return node;
    }

    private Node addDepivotNode(Node node, List<SourceColumn> sourceColumns)
            throws JsonParseException, JsonMappingException, IOException {
        DepivotFieldMapping mapping = null;

        for (SourceColumn sourceColumn : sourceColumns) {
            if (sourceColumn.getCalculation() == Calculation.DEPIVOT) {
                ObjectMapper om = new ObjectMapper();
                mapping = om.readValue(sourceColumn.getArguments(), DepivotFieldMapping.class);
                break;
            }
        }

        return node.depivot(mapping.getTargetFields(), mapping.getSourceFieldTuples());
    }

    private Node addTruncateLogic(Node node, List<SourceColumn> sourceColumns) {
        for (SourceColumn column : sourceColumns) {
            if (ColumnTypeConverter.isStringField(column.getColumnType())) {
                node = addTruncateNode(node, column.getColumnName(),
                        ColumnTypeConverter.getStringFieldSize(column.getColumnType()));
            }
        }
        return node;
    }

    private Node addTruncateNode(Node node, String columnName, int maxLength) {
        Function<?> function = new StringTruncateFunction(columnName, maxLength);
        node = node.apply(function, new FieldList(columnName), new FieldMetadata(columnName, String.class));
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