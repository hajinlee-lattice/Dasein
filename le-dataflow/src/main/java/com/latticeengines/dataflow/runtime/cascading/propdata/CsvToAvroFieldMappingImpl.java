package com.latticeengines.dataflow.runtime.cascading.propdata;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.Schema;

import com.latticeengines.domain.exposed.propdata.manage.SourceColumn;
import com.latticeengines.domain.exposed.propdata.manage.SourceColumn.Calculation;

import cascading.tuple.coerce.Coercions.Coerce;

public class CsvToAvroFieldMappingImpl implements CsvToAvroFieldMapping, Serializable {
    private static final long serialVersionUID = -4870380826193940885L;

    private static final String SCHEMA_DOC_STR = "Avro schema for ";
    private static final String DEFAULT_STR = "default";
    private static final String TABLE_NAME_STR = "tableName";
    private static final String FIELDS_STR = "fields";
    private static final String DOC_STR = "doc";
    private static final String RECORD_STR = "record";
    private static final String NAME_STR = "name";
    private static final String TYPE_STR = "type";
    private static final String NEWLINE = "\n";
    private static final String LEFT_CURLY_BRACKET = "{";
    private static final String RIGHT_CURLY_BRACKET = "}";
    private static final String RIGHT_BRACKET = " ]";
    private static final String LEFT_BRACKET = "[ ";
    private static final String NULL_STR = "null";
    private static final String COLON = ": ";
    private static final String COMMA = ", ";
    private static final String DOUBLE_QUOTE = "\"";

    private List<SourceColumn> columns;
    private List<FieldTuple> fieldTuples;
    private Map<String, String> csvToAvroFieldMap;
    private Map<String, String> avroToCsvFieldMap;
    private Map<String, Coerce<?>> csvFieldToTypeMap;

    public CsvToAvroFieldMappingImpl(List<SourceColumn> columns) {
        this.columns = columns;
        initTuples();
        initMaps();
    }

    @Override
    public String getAvroFieldName(String csvFieldName) {
        return csvToAvroFieldMap.get(csvFieldName);
    }

    @Override
    public String getCsvFieldName(String avroFieldName) {
        return avroToCsvFieldMap.get(avroFieldName);
    }

    @Override
    public Coerce<?> getFieldType(String csvFieldName) {
        return csvFieldToTypeMap.get(csvFieldName);
    }

    @Override
    public Schema getAvroSchema() {
        return generateSchema();
    }

    private void initTuples() {
        fieldTuples = new ArrayList<>();
        for (SourceColumn column : columns) {
            if (column.getCalculation() == Calculation.COLUMN_NAME_MAPPING) {
                Coerce<?> fieldType = ColumnTypeConverter.getCoercionFromTypeName(column.getColumnType());
                fieldTuples.add(new FieldTuple(column.getArguments(), column.getColumnName(), fieldType));
            }

        }
    }

    private void initMaps() {
        csvToAvroFieldMap = new HashMap<String, String>();
        avroToCsvFieldMap = new HashMap<String, String>();
        csvFieldToTypeMap = new HashMap<String, Coerce<?>>();

        populateMaps();
    }

    private void populateMaps() {
        for (FieldTuple tuple : fieldTuples) {
            csvFieldToTypeMap.put(tuple.getCsvFieldName(), tuple.getFieldType());
            csvToAvroFieldMap.put(tuple.getCsvFieldName(), tuple.getAvroFieldName());
            avroToCsvFieldMap.put(tuple.getAvroFieldName(), tuple.getCsvFieldName());
        }
    }

    private Schema generateSchema() {
        StringBuilder avroSchemaBuilder = new StringBuilder();
        String sourceName = columns.get(0).getSourceName();
        avroSchemaBuilder.append(LEFT_CURLY_BRACKET).append(DOUBLE_QUOTE).append(DOC_STR).append(DOUBLE_QUOTE)
                .append(COLON).append(DOUBLE_QUOTE).append(SCHEMA_DOC_STR).append(sourceName).append(DOUBLE_QUOTE)
                .append(COMMA).append(DOUBLE_QUOTE).append(TABLE_NAME_STR).append(DOUBLE_QUOTE).append(COLON)
                .append(DOUBLE_QUOTE).append(sourceName).append(DOUBLE_QUOTE).append(COMMA).append(DOUBLE_QUOTE)
                .append(TYPE_STR).append(DOUBLE_QUOTE).append(COLON).append(DOUBLE_QUOTE).append(RECORD_STR)
                .append(DOUBLE_QUOTE).append(COMMA).append(DOUBLE_QUOTE).append(NAME_STR).append(DOUBLE_QUOTE)
                .append(COLON).append(DOUBLE_QUOTE).append(sourceName).append(DOUBLE_QUOTE).append(COMMA)
                .append(DOUBLE_QUOTE).append(FIELDS_STR).append(DOUBLE_QUOTE).append(COLON).append(LEFT_BRACKET);

        int idx = 0;
        for (SourceColumn column : columns) {
            String avroType = ColumnTypeConverter.getAvroTypeNameFromCoercion(getFieldType(column.getArguments()));
            avroSchemaBuilder.append(LEFT_CURLY_BRACKET).append(DOUBLE_QUOTE).append(NAME_STR).append(DOUBLE_QUOTE)
                    .append(COLON).append(DOUBLE_QUOTE).append(column.getColumnName()).append(DOUBLE_QUOTE)
                    .append(COMMA).append(DOUBLE_QUOTE).append(TYPE_STR).append(DOUBLE_QUOTE).append(COLON)
                    .append(LEFT_BRACKET).append(DOUBLE_QUOTE).append(NULL_STR).append(DOUBLE_QUOTE).append(COMMA)
                    .append(DOUBLE_QUOTE).append(avroType).append(DOUBLE_QUOTE).append(RIGHT_BRACKET).append(COMMA)
                    .append(DOUBLE_QUOTE).append(DEFAULT_STR).append(DOUBLE_QUOTE).append(COLON).append(NULL_STR)
                    .append(RIGHT_CURLY_BRACKET);
            if (++idx < columns.size()) {
                avroSchemaBuilder.append(COMMA);
            }
            avroSchemaBuilder.append(NEWLINE);
        }
        avroSchemaBuilder.append(RIGHT_BRACKET).append(RIGHT_CURLY_BRACKET);
        return new Schema.Parser().parse(avroSchemaBuilder.toString());
    }

    class FieldTuple implements Serializable {
        private static final long serialVersionUID = 8195760208322606390L;
        private String csvFieldName;
        private String avroFieldName;
        private Coerce<?> fieldType;

        public FieldTuple(String csvFieldName, String avroFieldName, Coerce<?> fieldType) {
            this.csvFieldName = csvFieldName;
            this.avroFieldName = avroFieldName;
            this.fieldType = fieldType;
        }

        public String getCsvFieldName() {
            return csvFieldName;
        }

        public String getAvroFieldName() {
            return avroFieldName;
        }

        public Coerce<?> getFieldType() {
            return fieldType;
        }
    }

}
